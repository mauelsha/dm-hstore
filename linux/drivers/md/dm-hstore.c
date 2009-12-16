/*
 * Copyright (C) 2009 Red Hat GmbH. All rights reserved.
 *
 * Module Author: Heinz Mauelshagen <heinzm@redhat.com>
 *
 * This file is released under the GPL.
 *
 * Hierachical Storage (Caching) target to stack on top
 * of arbitrary other (readonly) block device (eg. iSCSI).
 *
 * Features:
 * o Caches reads and writes keeping persistent state metadata.
 * o Writes back in order to enhance streaming performance
 *   on fragmented access pattern.
 * o Can run on top of readonly original device (eg. on DVD-ROM)
 * o If so, writes back any dirty areas when set readwrite
 *   (useful for tests)
 * o readonly <-> readwrite access changes supported via message interface
 * o Initializes metadata for extents in cache in the background
 *   in order to fasten cache construction
 * o supports cache resizing via message interface or constructor
 o o barrier IO supported
 * o keeps metadata persistent by default
 * o stores CRCs with metadata for integrity checks
 * o stores versions with metadata to support future metadata migration
 *
 * Test features only:
 * o transient cache
 * o cache write through
 *
 *
 * Disk layout of backing store:
 *
 * H.D1.M1.D2.M2.D3.M3...
 *
 * H  : Header
 * Dn : data sectors belonging to n
 * Mn : metadata for n
 *
 * FIXME:
 * o support writeback/writethrough ctr argument in header flags
 * o support persistent/transient/invalidate ctr argument in header flags
 * o allow bio reads on dirty extents flushing out to the origin.
 *
 */

static const char *version = "0.127";

#include "dm.h"

#include <linux/dm-io.h>
#include <linux/dm-kcopyd.h>
#include <linux/bio.h>
#include <linux/blkdev.h>
#include <linux/crc32.h>
#include <linux/init.h>
#include <linux/module.h>
#include <linux/vmalloc.h>
#include <linux/workqueue.h>

#define	DM_MSG_PREFIX	"dm-hstore"
#define	DAEMON	DM_MSG_PREFIX	"d"

/* FIXME: factor these macros out to dm.h */
#define	TI_ERR_RET(str, ret) \
	do { ti->error = DM_MSG_PREFIX ": " str; return ret; } while (0);
#define	TI_ERR(str)	TI_ERR_RET(str, -EINVAL)
#define	DM_ERR_RET(ret, x...) \
	do { DMERR(x); return ret; } while (0);
#define	DM_ERR(x...)	DM_ERR_RET(-EINVAL, x)

#define	EXTENT_SECTORS_DEFAULT	to_sector(128*1024)	/* 128KB */
#define	SECTOR_SIZE		to_bytes(1)	/* Sector size in bytes. */

#define     range_ok(i, min, max)   (i >= min && i <= max)

/*
 * Fixed size of all ondisk metadata areas
 * (ie. header and per extent metadata).
 */
#define	META_SECTORS	to_sector(4096)
#define	EXTENTS_MIN	1
#define	MIN_CACHE_SIZE	(8 * 2 * 1024) /* 8MB */

/* Global hstore multi-threaded workqueue. */
static struct workqueue_struct *_hstore_wq;

/* A hash. */
struct hash {
	struct list_head *hash;
	unsigned buckets;
	unsigned mask;
};

/*
 * On disk metadata for cached extents.
 */
static const char *extent_magic = "DmHsToRe";
#define	EXTENT_MAGIC_SIZE 8
struct extent_disk {
	uint8_t magic[EXTENT_MAGIC_SIZE];	/* Magic key */
	uint32_t crc;
	uint32_t filler;
	uint64_t flags;		/* Status flag. */
	struct {
		uint64_t cache_offset;	/* Cache device sector offset. */
		uint64_t orig_offset;	/* Original (cached) device offset. */
	} addr;
} __attribute__ ((__packed__));

/*
 * Incore housekeeping of extents and ios from/to them
 * (those on cache device but not on cached device).
 */
struct extent {
	struct hstore_c *hc;

	/* FIXME: these lists consume quite some memory. */
	struct {
		struct list_head dirty;		/* Dirty */
		struct list_head endio_flush;	/* Endio + flush. */
		struct list_head hash;		/* Hash */
		struct list_head init_lru;	/* Extent to init or reusable */
		atomic_t endio_ref;	/* Used to put extent on endio list. */
	} lists;

	struct extent_io {
		struct bio_list in;	/* Input bios queued. */
		struct bio_list endio;	/* Bios to endio. */
		/* Protect against races in hstore_end_io(). */
		spinlock_t endio_lock;
		unsigned long flags;	/* Status flag */
	} io;

	/* Device addresses. */
	struct {
		union {
			sector_t offset;/* Sector offset on cache device. */
		} cache;
		union {
			sector_t key;	/* Hash key. */
			sector_t offset;/* Sector offset on original device. */
		} orig;
	} addr;

	/*
	 * Extent metadata on disk representation
	 * (allocated from mempool during IO only).
	 */
	struct extent_disk *disk;
};

/* Cache device header. */
static const char *header_magic = "dm_hstore_HM4711";
#define	HEADER_MAGIC_SIZE	16
struct disk_header {
	uint8_t magic[HEADER_MAGIC_SIZE];
	uint32_t crc;
	struct {
		uint8_t major;
		uint8_t minor;
		uint8_t subminor;
		uint8_t filler;
	} version;
	uint64_t dev_size;	/* Cache device size. */
	uint64_t extent_size;	/* Cache extent size. */
	uint64_t flags;		/* Feature flags. */
} __attribute__ ((__packed__));

/* Macros to access state flags. */
#define	BITOPS(name, what, var, flag) \
static inline int TestClear ## name ## what(struct var *v) \
{ return test_and_clear_bit(flag, &v->io.flags); } \
static inline int TestSet ## name ## what(struct var *v) \
{ return test_and_set_bit(flag, &v->io.flags); } \
static inline void Clear ## name ## what(struct var *v) \
{ clear_bit(flag, &v->io.flags); } \
static inline void Set ## name ## what(struct var *v) \
{ set_bit(flag, &v->io.flags); } \
static inline int name ## what(struct var *v) \
{ return test_bit(flag, &v->io.flags); }

/* Extent state flags. */
enum extent_flags {
	EXTENT_BIO_ACTIVE,	/* Bios active on extent. */
	EXTENT_BIO_WRITE,	/* Write bios submitted. */
	EXTENT_COPY_ACTIVE,	/* kcopyd active on extent. */
	EXTENT_DIRTY,		/* Extent needs writing back to origin. */
	EXTENT_ERROR,		/* IO error on extent ocurred. */
	EXTENT_FREE,		/* Extent is available. */
	EXTENT_META_READ,	/* Extent metadata read. */
	EXTENT_UPTODATE,	/* Extent data is uptodate. */
	EXTENT_WRITING_FREE,	/* Writing next free metadata header. */
};

BITOPS(Extent, BioActive, extent, EXTENT_BIO_ACTIVE)
BITOPS(Extent, BioWrite, extent, EXTENT_BIO_WRITE)
BITOPS(Extent, CopyActive, extent, EXTENT_COPY_ACTIVE)
BITOPS(Extent, Dirty, extent, EXTENT_DIRTY)
BITOPS(Extent, Error, extent, EXTENT_ERROR)
BITOPS(Extent, Free, extent, EXTENT_FREE)
BITOPS(Extent, MetaRead, extent, EXTENT_META_READ)
BITOPS(Extent, Uptodate, extent, EXTENT_UPTODATE)
BITOPS(Extent, WritingFree, extent, EXTENT_WRITING_FREE)

/* REMOVEME: */
/* Development statistics. */
struct stats {
	atomic_t io[2];
	atomic_t deferred_io[2];
	atomic_t submitted_io[2];
	atomic_t extent_copy_active;
	atomic_t extent_data_io[2];
	atomic_t extent_meta_io[2];
	atomic_t extent_valid;
	atomic_t extent_dirty;
	atomic_t extent_clear_uptodate;
	atomic_t hits;
	atomic_t misses;
	atomic_t overwrite;
	atomic_t write_free;
	atomic_t writing_free;
};

/* Reset statistics variables. */
static void stats_init(struct stats *stats)
{
	int i = 2;

	while (i--) {
		atomic_set(stats->io + i, 0);
		atomic_set(stats->deferred_io + i, 0);
		atomic_set(stats->submitted_io + i, 0);
		atomic_set(stats->extent_data_io + i, 0);
		atomic_set(stats->extent_meta_io + i, 0);
	}

	atomic_set(&stats->extent_copy_active, 0);
	atomic_set(&stats->extent_valid, 0);
	atomic_set(&stats->extent_dirty, 0);
	atomic_set(&stats->extent_clear_uptodate, 0);
	atomic_set(&stats->hits, 0);
	atomic_set(&stats->misses, 0);
	atomic_set(&stats->overwrite, 0);
	atomic_set(&stats->write_free, 0);
	atomic_set(&stats->writing_free, 0);
}

/* Create new or open existing cache. */
enum handle_type {
	AUTO_CACHE = 0,	/* Auto cache discovery (open existing/ create new). */
	CREATE_CACHE,	/* Force create new cache. */
	OPEN_CACHE	/* Open existing cache. */
};

/* Maps a range of a device. */
struct c_dev {
	struct dm_dev *dev;
	sector_t start;
	sector_t size;
	int mode;
};

/* Cache context. */
struct hstore_c {
	/* Extent hash. */
	struct hash hash;

	struct extent *extent_mru;	/* Most recently used extent. */

	struct {
		spinlock_t lock;	/* Protects central input list above. */
		struct bio_list in;	/* Pending bios (central input list). */
		struct bio_list defer;	/* Deferred bios (no extents). */
		atomic_t ref;	/* IO in flight reference counting. */

		wait_queue_head_t suspendq;	/* Suspend synchronization. */
		wait_queue_head_t invalidateq;	/* Invalidate synchronization.*/

		struct work_struct ws;	/* IO work. */

		/* IO services used. */
		struct dm_io_client *dm_io_client;
		struct dm_kcopyd_client *kcopyd_client;

		/* Mempool of metadata sectors. */
		mempool_t *metadata_pool;

		unsigned long flags;
	} io;

	/* Extent address masks to quicken calculation... */
	sector_t extent_mask;		/* of hash key. */
	sector_t extent_mask_inv;	/* of extent address. */

	/* Cache and original device properties. */
	struct devs {
		struct c_dev cache;
		struct c_dev orig;
	} devs;

	/* Extent properties. */
	struct {
		sector_t size;
		atomic_t initialized;
		atomic_t total;
		atomic_t free;
	} extents;

	struct hstore_lists {
		/* List of drity extents. */
		struct list_head dirty;

		/* List of extents to end io in worker. */
		struct list_head endio;

		/* List of extents to io in do_flush(). */
		struct list_head flush;

		/* List of extents to initialize in do_extents_init(). */
		struct list_head init;

		/* List of free/LRU ondisk cache extents. */
		struct list_head lru;

		struct {
			spinlock_t endio; /* Protect endio list */
			spinlock_t lru;   /* Protect lru list. */
		} locks;
	} lists;

	/* ctr parameters for status output. */
	struct {
		unsigned params;
		enum handle_type handle;
		sector_t cache_size;
		sector_t cache_new_size;
		sector_t extent_size;
		int write_policy;
		int store_policy;
	} params;

	struct stats stats;
	struct dm_target *ti;

	/* Cache device disk header. */
	struct disk_header *disk_header;
};

/* Cache state flags. */
enum hstore_c_flags {
	CACHE_BARRIER,			/* Cache is performing barrier IO. */
	CACHE_CHANGE_PERSISTENCY,	/* Toggle cache persistency. */
	CACHE_CHANGE_RW,		/* Toggle origin rw access. */
	CACHE_CHANGE_WRITE_POLICY,	/* Toggle cache write policy. */
	CACHE_DIRTY_FLUSHING,		/* Cache is flushing dirty extents. */
	CACHE_INITIALIZING,		/* Cache initialization of extents. */
	CACHE_INITIALIZING_NEW,		/* Write " */
	CACHE_INITIALIZATION_ACTIVE,	/* Initialization IO active. */
	CACHE_INVALIDATE,		/* Invalidate cache. */
	CACHE_PERSISTENT,		/* Cache persistent. */
	CACHE_RESIZE,			/* Cache resizing. */
	CACHE_SUSPEND,			/* Cache suspension. */
	CACHE_WRITE_THROUGH,		/* Write through cache. */
	CACHE_WRITING_FREE,		/* Writing next free header. */
	CACHE_THREAD_RUNNING,		/* Thread running. */
};

BITOPS(Cache, Barrier, hstore_c, CACHE_BARRIER)
BITOPS(Cache, ChangePersistency, hstore_c, CACHE_CHANGE_PERSISTENCY)
BITOPS(Cache, ChangeRW, hstore_c, CACHE_CHANGE_RW)
BITOPS(Cache, ChangeWritePolicy, hstore_c, CACHE_CHANGE_WRITE_POLICY)
BITOPS(Cache, DirtyFlushing, hstore_c, CACHE_DIRTY_FLUSHING)
BITOPS(Cache, Initializing, hstore_c, CACHE_INITIALIZING)
BITOPS(Cache, InitializingNew, hstore_c, CACHE_INITIALIZING_NEW)
BITOPS(Cache, InitializationActive, hstore_c, CACHE_INITIALIZATION_ACTIVE)
BITOPS(Cache, Invalidate, hstore_c, CACHE_INVALIDATE)
BITOPS(Cache, Persistent, hstore_c, CACHE_PERSISTENT)
BITOPS(Cache, Resize, hstore_c, CACHE_RESIZE)
BITOPS(Cache, Suspend, hstore_c, CACHE_SUSPEND)
BITOPS(Cache, WriteThrough, hstore_c, CACHE_WRITE_THROUGH)
BITOPS(Cache, WritingFree, hstore_c, CACHE_WRITING_FREE)
BITOPS(Cache, ThreadRunning, hstore_c, CACHE_THREAD_RUNNING)
#undef BITOPS
/*
 * Disk metadata sectors alloc/free.
 */
static void *metadata_alloc(struct hstore_c *hc)
{
	return mempool_alloc(hc->io.metadata_pool, GFP_NOIO);
}

static void metadata_free(struct hstore_c *hc, void *disk_header)
{
	mempool_free(disk_header, hc->io.metadata_pool);
}

/*
 * Extent struct allocation/free.
 */
static void *extent_alloc(struct hstore_c *hc, gfp_t flags)
{
	struct extent *extent = kzalloc(sizeof(*extent), flags);

	if (extent) {
		extent->hc = hc;
		bio_list_init(&extent->io.endio);
		spin_lock_init(&extent->io.endio_lock);
		bio_list_init(&extent->io.in);
		INIT_LIST_HEAD(&extent->lists.dirty);
		INIT_LIST_HEAD(&extent->lists.endio_flush);
		INIT_LIST_HEAD(&extent->lists.hash);
		INIT_LIST_HEAD(&extent->lists.init_lru);
	}

	return extent;
}

static void extent_free(struct extent *extent)
{
	kfree(extent);
}

/* Return extent for position in init/LRU list. */
static inline struct extent *extent_pos(struct list_head *pos)
{
	return list_entry(pos, struct extent, lists.init_lru);
}

/* Return extent size (extent sectors + metadata sectors). */
static inline sector_t extent_size(struct hstore_c *hc)
{
	return hc->extents.size + META_SECTORS;
}

/* Return start of first extent. */
static inline sector_t extents_start(struct hstore_c *hc)
{
	return hc->devs.cache.start + META_SECTORS;
}

/* Return total number of extents. */
static sector_t extents_total(struct hstore_c *hc)
{
	sector_t extents = hc->devs.cache.size - extents_start(hc);

	do_div(extents, extent_size(hc));
	return extents;
}

/* Factor out to dm.c */
static int multiple(sector_t a, sector_t b)
{
	sector_t r = a;

	do_div(r, b);
	return a == r * b;
}

/* Check no ios inflight. */
static int cc_ios_inflight(struct hstore_c *hc)
{
	return atomic_read(&hc->io.ref) ||
	       !list_empty(&hc->lists.endio) ||
	       !list_empty(&hc->lists.flush);
}

/* Check cache clean (ie. all got flushed but deferred input io) */
static int cc_cache_clean(struct hstore_c *hc)
{
	return !cc_ios_inflight(hc) && list_empty(&hc->lists.dirty);
}

/* Check cache idle. */
static int cc_idle(struct hstore_c *hc)
{
	return !cc_ios_inflight(hc) &&
	       bio_list_empty(&hc->io.defer) &&
	       bio_list_empty(&hc->io.in);
}

/* Derive hash key from bio sector. */
static inline sector_t _bio_to_key(struct hstore_c *hc, struct bio *bio)
{
	return bio->bi_sector & hc->extent_mask;
}

/* Derive offset within extent from bio. */
static inline sector_t bio_to_extent(struct hstore_c *hc, struct bio *bio)
{
	return bio->bi_sector & hc->extent_mask_inv;
}

/*
 * Remap bio:
 * o to origin in case of write through cache
 * -or-
 * o to sector offset relative to extent start on cache
 *   device in case of write back cache.
 */
static inline void bio_remap(struct extent *extent, struct bio *bio)
{
	struct hstore_c *hc = extent->hc;

	if (CacheWriteThrough(hc) && bio_data_dir(bio) == WRITE)
		bio->bi_bdev = hc->devs.orig.dev->bdev;
	else {
		bio->bi_bdev = hc->devs.cache.dev->bdev;
		bio->bi_sector = extent->addr.cache.offset +
				 bio_to_extent(hc, bio);
	}
}

/* Initialize a hash. */
static int hash_init(struct hstore_c *hc)
{
	unsigned buckets;
	sector_t extents = extents_total(hc);
	struct hash *hash = &hc->hash;

	/* 32 entries per bucket average. */
	do_div(extents, 32);
	buckets = roundup_pow_of_two(extents);

	/* Allocate stripe hash */
	hash->hash = vmalloc(buckets * sizeof(*hash->hash));
	if (!hash->hash)
		return -ENOMEM;

	hash->buckets = buckets;
	hash->mask = buckets - 1;

	/* Initialize buckets. */
	while (buckets--)
		INIT_LIST_HEAD(hash->hash + buckets);

	return 0;
}

static void hash_exit(struct hash *hash)
{
	if (hash->hash)
		vfree(hash->hash);
}

/* Return bucket within hash. */
static inline struct list_head *hash_bucket(struct hash *hash,
					    sector_t key)
{
	return hash->hash +
	    ((unsigned) ((key * 2654435387U) >> 12) & hash->mask);
}

/* Insert an entry into a hash. */
static inline void hash_insert(struct hash *hash, struct extent *extent)
{
	list_add_tail(&extent->lists.hash,
		      hash_bucket(hash, extent->addr.orig.key));
}

/* Lookup an extent in the hash. */
static struct extent *hash_lookup(struct hstore_c *hc, sector_t key)
{
	struct list_head *bucket = hash_bucket(&hc->hash, key);
	struct extent *extent;

	list_for_each_entry(extent, bucket, lists.hash) {
		if (key == extent->addr.orig.key)
			return extent;
	}

	return NULL;
}

/* Wake worker on one CPU only! */
static void do_hstore(struct work_struct *ws);
static void wake_do_hstore(struct hstore_c *hc)
{
	queue_work(_hstore_wq, &hc->io.ws);
}

/* Get an IO reference for endio processing. */
static inline void endio_get(struct extent *extent)
{
	atomic_inc(&extent->hc->io.ref);
	atomic_inc(&extent->lists.endio_ref);
}

/* Transfer core extent representation to disk. */
static void extent_to_disk(struct extent *extent)
{
	struct extent_disk *ed = extent->disk;

	strncpy(ed->magic, extent_magic, sizeof(*ed->magic));
	ed->flags = cpu_to_le64(extent->io.flags);
	ed->crc = ed->filler = 0;
	ed->addr.cache_offset = cpu_to_le64(extent->addr.cache.offset);
	ed->addr.orig_offset = cpu_to_le64(extent->addr.orig.offset);
	ed->crc = cpu_to_le32(crc32(~0, ed, sizeof(*ed)));
}

/* Transfer disk extent representation to core. */
static void extent_to_core(struct extent *extent)
{
	struct extent_disk *ed = extent->disk;

	extent->io.flags = le64_to_cpu(ed->flags);
	extent->addr.cache.offset = le64_to_cpu(ed->addr.cache_offset);
	extent->addr.orig.offset = le64_to_cpu(ed->addr.orig_offset);
	ed->crc = le32_to_cpu(ed->crc);
}

/* Check extent magic. */
static int extent_check(struct extent *extent, sector_t offset)
{
	struct hstore_c *hc = extent->hc;
	struct extent_disk *ed = extent->disk;
	unsigned crc = ed->crc;

	ed->crc = 0;
	if (strncmp(ed->magic, extent_magic, sizeof(*ed->magic)) ||
	    crc != crc32(~0, ed, sizeof(*ed)) ||
	    ed->filler ||
	    extent->addr.cache.offset < hc->devs.cache.start ||
	    extent->addr.cache.offset > hc->devs.cache.size ||
	    extent->addr.orig.offset < hc->devs.orig.start ||
	    extent->addr.orig.offset > hc->devs.orig.size ||
	    extent->addr.cache.offset != offset)
		return -EINVAL;

	return 0;
}

/* Push an extent to the end of the endio list. */
static void endio_put(struct extent *extent)
{
	struct hstore_c *hc = extent->hc;

	/* Push to the list in case last IO reference gets dropped. */
	if (atomic_dec_and_test(&extent->lists.endio_ref)) {
		int wake;
		unsigned long flags;

		/* Put extent on endio list for worker to handle. */
		spin_lock_irqsave(&hc->lists.locks.endio, flags);
		BUG_ON(!list_empty(&extent->lists.endio_flush));
		wake = list_empty(&extent->lists.endio_flush);
		if (wake)
			list_add_tail(&extent->lists.endio_flush,
				      &hc->lists.endio);
		spin_unlock_irqrestore(&hc->lists.locks.endio, flags);

		/* Wakeup worker to deal with endio list. */
		if (wake)
			wake_do_hstore(hc);
	}

	atomic_dec(&hc->io.ref);
}

/* Pop an extent off the endio list. */
static struct extent *extent_endio_pop(struct hstore_c *hc)
{
	struct extent *extent;

	spin_lock_irq(&hc->lists.locks.endio);
	if (list_empty(&hc->lists.endio))
		extent = NULL;
	else {
		extent = list_first_entry(&hc->lists.endio,
					  struct extent, lists.endio_flush);
		list_del_init(&extent->lists.endio_flush);
	}

	spin_unlock_irq(&hc->lists.locks.endio);
	return extent;
}

/* Pop an extent off the flush list. */
static struct extent *extent_flush_pop(struct hstore_c *hc)
{
	struct extent *extent;

	if (list_empty(&hc->lists.flush))
		extent = NULL;
	else {
		extent = list_first_entry(&hc->lists.flush,
					  struct extent, lists.endio_flush);
		list_del_init(&extent->lists.endio_flush);
	}

	return extent;
}

/* Pop an extent off the init list. */
static struct extent *extent_init_pop(struct hstore_c *hc)
{
	struct extent *extent;

	if (list_empty(&hc->lists.init))
		extent = NULL;
	else {
		extent = list_first_entry(&hc->lists.init,
					  struct extent, lists.init_lru);
		list_del_init(&extent->lists.init_lru);
	}

	return extent;
}

/* Endio function for io() and kcopy_endio(). */
static void endio(unsigned long error, void *context)
{
	struct extent *extent = context;

	if (unlikely(error))
		SetExtentError(extent);

	endio_put(extent);
}

/* Handle kcopyd endio state for copied extents. */
static void kcopy_endio(int read_err, unsigned long write_err, void *context)
{
	endio((unsigned long) (read_err || write_err), context);
}

/* (A)synchronuous IO. If fn != NULL asynchronuous IO, else synchronuous. */
static int io(struct hstore_c *hc, void *ptr, unsigned count,
	      sector_t sector, int rw, io_notify_fn fn, void *context)
{
	struct dm_io_region region = {
		.bdev = hc->devs.cache.dev->bdev,
		.sector = sector,
		.count = count,
	};
	struct dm_io_request control = {
		.bi_rw = rw,
		.mem.type = DM_IO_KMEM,
		.mem.ptr.addr = ptr,
		.notify.fn = fn,
		.notify.context = context,
		.client = hc->io.dm_io_client,
	};

	return dm_io(&control, 1, &region, NULL);
}

/* Read/write a cache device extent struct */
static int extent_meta_io(struct hstore_c *hc, struct extent *extent,
			  int rw)
{
	/* Removeme: statistics. */
	atomic_inc(hc->stats.extent_meta_io + (rw == WRITE));
	return io(hc, extent->disk, 1,
		  extent->addr.cache.offset + hc->extents.size,
		  rw, endio, extent);
}

/* Read/write the cache device header synchronuously. */
static int header_io(struct hstore_c *hc, int rw)
{
	return io(hc, hc->disk_header, 1,
		  hc->devs.cache.start, rw, NULL, NULL);
}

/* Transfer cache device header from/to CPU. */
static void header_to_disk(struct hstore_c *hc)
{
	struct disk_header *dh = hc->disk_header;

	dh->crc = 0;
	dh->crc = cpu_to_le32(crc32(~0, dh, sizeof(*dh)));
	dh->dev_size = cpu_to_le64(dh->dev_size);
	dh->extent_size = cpu_to_le64(dh->extent_size);
	dh->flags = cpu_to_le64(dh->flags);
}

static void header_to_cpu(struct hstore_c *hc)
{
	struct disk_header *dh = hc->disk_header;

	dh->crc = le32_to_cpu(dh->crc);
	dh->dev_size = le64_to_cpu(dh->dev_size);
	dh->extent_size = le64_to_cpu(dh->extent_size);
}

/* Initialize disk header version. */
static void header_version_init(struct disk_header *dh)
{
	dh->version.major = 1;
	dh->version.minor = 0;
	dh->version.subminor = 0;
	dh->version.filler = 0;
}

/* Initialize cache device header */
static void header_init(struct hstore_c *hc)
{
	struct disk_header *dh = hc->disk_header;

	strncpy(dh->magic, header_magic, sizeof(*dh->magic));
	header_version_init(dh);
	dh->dev_size = hc->devs.cache.size;
	dh->extent_size = hc->extents.size;
	dh->flags = hc->io.flags & (CACHE_PERSISTENT | CACHE_WRITE_THROUGH);
}

/* Check that disk header version's right. */
static int header_version_check(struct disk_header *dh)
{
	return dh->version.major == 1 &&
	       !dh->version.minor &&
	       !dh->version.subminor &&
	       !dh->version.filler;
}

/* Write new cache device header. */
static int header_write(struct hstore_c *hc)
{
	int r;

	header_init(hc);
	header_to_disk(hc);
	r = header_io(hc, WRITE);
	header_to_cpu(hc);

	return r;
}

/* Check cache device header validity. */
static int header_check(struct hstore_c *hc)
{
	int r, hm;
	struct disk_header *dh = hc->disk_header;
	unsigned crc = dh->crc;

	dh->crc = 0;
	hm = strncmp(dh->magic, header_magic, sizeof(*dh->magic));
	r = hm || !header_version_check(dh) ||
	    crc != crc32(~0, dh, sizeof(*dh)) ||
	    !dh->dev_size || !dh->extent_size ? -EINVAL : 0;

	return (r && !hm) ? -EPERM : r;
}

static void extent_init(struct hstore_c *hc, struct extent *extent,
			sector_t offset)
{
	extent->addr.cache.offset = hc->devs.cache.start + offset;
	extent->addr.orig.offset = 0;
	extent->io.flags = 0;
	SetExtentFree(extent);
}

/* End IO a bio. */
static inline void _bio_endio(struct bio *bio, int error)
{
	/* bio_endio(bio, bio->bi_size, error); */
	bio_endio(bio, error);
}

/* bio_endio() a bio_list .*/
static void _bio_endio_list(struct bio_list *list, int error)
{
	struct bio *bio;

	while ((bio = bio_list_pop(list)))
		_bio_endio(bio, error);
}

/* Add an element to a list safely. */
static inline void _extent_add(struct list_head *from,
			       struct list_head *to)
{
	if (list_empty(from))
		list_add_tail(from, to);
}

/* Delete an element from a list safely. */
static inline void _extent_del(struct list_head *list)
{
	if (!list_empty(list))
		list_del_init(list);
}

/* Add extent to end of init list. */
static inline void extent_init_add(struct extent *extent)
{
	_extent_add(&extent->lists.init_lru, &extent->hc->lists.init);
}

/* Add extent to end of dirty list. */
static inline void extent_dirty_add(struct extent *extent)
{
	_extent_add(&extent->lists.dirty, &extent->hc->lists.dirty);
}

/* Add extent to end of LRU list. */
static inline void extent_lru_add(struct extent *extent)
{
	struct hstore_lists *lists = &extent->hc->lists;

	spin_lock_irq(&lists->locks.lru);
	_extent_add(&extent->lists.init_lru, &lists->lru);
	spin_unlock_irq(&lists->locks.lru);
}

/* Remove extent from LRU list. */
static void extent_lru_del(struct extent *extent)
{
	struct hstore_lists *lists = &extent->hc->lists;

	spin_lock_irq(&lists->locks.lru);
	_extent_del(&extent->lists.init_lru);
	spin_unlock_irq(&lists->locks.lru);
}

/* Pop an extent off the LRU list. */
struct extent *extent_lru_pop(struct hstore_c *hc)
{
	struct hstore_lists *lists = &hc->lists;
	struct extent *extent;

	spin_lock_irq(&lists->locks.lru);
	if (list_empty(&hc->lists.lru))
		extent = NULL;
	else {
		extent = list_first_entry(&hc->lists.lru,
					  struct extent, lists.init_lru);
		list_del_init(&extent->lists.init_lru);
	}

	spin_unlock_irq(&lists->locks.lru);
	return extent;
}

/* Add extent to end of flush list. */
static void extent_flush_add(struct extent *extent)
{
	_extent_add(&extent->lists.endio_flush, &extent->hc->lists.flush);
}

/* Safely delete an extent from the hash. */
static void extent_hash_del(struct extent *extent)
{
	_extent_del(&extent->lists.hash);
}

/* Insert an entry into the extent hash. */
static void extent_insert(struct extent *extent)
{
	extent_hash_del(extent);
	hash_insert(&extent->hc->hash, extent);	/* Hash the extent. */
}

/* Original device writable ? */
static inline int OrigWritable(struct hstore_c *hc)
{
	return hc->devs.orig.mode & FMODE_WRITE;
}

/*
 * Write the next free extent metadata header in
 * case a free one got allocated from the LRU list.
 */
static void write_free_header(struct hstore_c *hc)
{
	struct extent *extent;

	/* The LRU list may not be empty to write another free extent. */
	BUG_ON(list_empty(&hc->lists.lru));
	extent = extent_pos(hc->lists.lru.next);
	BUG_ON(!ExtentFree(extent));

	/* REMOVEME: statistics */
	atomic_inc(&hc->stats.write_free);
	extent->disk = metadata_alloc(hc);
	extent_to_disk(extent);
	SetExtentWritingFree(extent);
	SetCacheWritingFree(hc);

	/* Take out metadata IO reference. */
	endio_get(extent);
	BUG_ON(extent_meta_io(hc, extent, WRITE));
}

/* Try to get an extent either from the hash or off the LRU list. */
static struct extent *extent_get(struct hstore_c *hc, struct bio *bio)
{
	/* key is relative (0 based) start address of extent on origin. */
	sector_t key = _bio_to_key(hc, bio);
	struct extent *extent;

	/* Try to look extent up in the hash. */
	extent = hash_lookup(hc, key);
	if (extent) {
		/*
		 * Take off LRU list in case it is on,
		 * because we'll add bios to the extent
		 * and initiate IO on it now.
		 */
		extent_lru_del(extent);
		goto out;
	}

	/*
	 * Don't try fetching an extent from the LRU list and
	 * write a new free one while we're still initializing.
	 */
	if (CacheInitializing(hc))
		goto out;

	/*
	 * Can't fetch another free extent off the LRU list and
	 * order the following free extent metadata to be written
	 * out while another extents metadata IO is still active.
	 */
	if (CacheWritingFree(hc)) {
		/* REMOVEME: statistics */
		atomic_inc(&hc->stats.writing_free);
		goto out;
	}

	/* Try to fetch an extent off the LRU list. */
	extent = extent_lru_pop(hc);
	if (extent) {
		/* REMOVEME: */
		BUG_ON(!bio_list_empty(&extent->io.in));

		if (TestClearExtentFree(extent) &&
		    CachePersistent(hc) &&
		    !atomic_dec_and_test(&hc->extents.free))
			/* Write next free extent header */
			write_free_header(hc);

		/*
		 * Clear state flags but error, because the extent
		 * is going to be reused with a different key.
		 */
		if (unlikely(ExtentError(extent))) {
			extent->io.flags = 0;
			SetExtentError(extent);
		} else
			extent->io.flags = 0;

		/* Adjust key and insert into hash. */
		extent->addr.orig.key = hc->devs.orig.start + key;
		extent_insert(extent);
	}

out:
	return extent;
}

/* Copy data across between origin and cache extents. */
static int extent_data_io(struct hstore_c *hc, struct extent *extent, int rw)
{
	struct dm_io_region cache = {
		.bdev = hc->devs.cache.dev->bdev,
		.sector = extent->addr.cache.offset,
	}, orig = {
		.bdev = hc->devs.orig.dev->bdev,
		.sector = extent->addr.orig.offset,
	}, *from, *to;

	/* Check for partial extent at origin device end. */
	cache.count = orig.count =
	    min(hc->extents.size, hc->devs.orig.size - orig.sector);

	rw == READ ? (from = &orig,  to = &cache) :
		     (from = &cache, to = &orig);
	return dm_kcopyd_copy(hc->io.kcopyd_client, from, 1, to, 0,
			      kcopy_endio, extent);
}

/* Check if the actual extent is invalid. */
static struct extent *extent_valid(struct extent *extent,
					  struct bio *bio)
{
	if (extent &&
	    extent->hc->devs.orig.start + _bio_to_key(extent->hc, bio) ==
	    extent->addr.orig.offset) {
		/* REMOVEME: statistics */
		atomic_inc(&extent->hc->stats.extent_valid);
		return extent;
	}

	return NULL;
}

/* Defer bios when no extents available and update statistics. */
static void _bio_defer(struct hstore_c *hc, struct bio *bio)
{
	/* REMOVEME: statistics */
	atomic_inc(hc->stats.deferred_io + (bio_data_dir(bio) == WRITE));
	bio_list_add(&hc->io.defer, bio);
}

/*
 * Change hstore settings (cache persistency, rw access
 * and cache write policy) on message interface request.
 */
static void do_settings(struct hstore_c *hc)
{
	int r = 0;

	if (TestClearCacheChangePersistency(hc)) {
		if (CachePersistent(hc))
			ClearCachePersistent(hc);
		else
			SetCachePersistent(hc);

		r++;
	}

	if (TestClearCacheChangeRW(hc)) {
		if (OrigWritable(hc))
			hc->devs.orig.mode &= ~FMODE_WRITE;
		else
			hc->devs.orig.mode |= FMODE_WRITE;

		r++;
	}

	if (TestClearCacheChangeWritePolicy(hc)) {
		if (CacheWriteThrough(hc))
			ClearCacheWriteThrough(hc);
		else
			SetCacheWriteThrough(hc);

		r++;
	}

	if (r && CachePersistent(hc))
		header_write(hc);
}

/*
 * Flush any extents on flush list in/out from/to cache device.
 *
 *   o cold in cache -> read it from origin into cache
 *   o uptodate and clean in cache -> read/write to/from it in cache;
 *     set it dirty in case of writes
 *   o uptodate and dirty in cache -> write it out from cache to origin
 */
/* IO whole extents (ie. copy data accross). */
static void extent_io(struct hstore_c *hc, struct extent *extent, int rw)
{
	/*
	 * Update state flags, allocate ondisk structure, transfer them
	 * to it and restore them (they will get updated in do_endios()).
	 */
	if (CachePersistent(hc)) {
		/* Save flags to restore. */
		unsigned long flags = extent->io.flags;

		rw == READ ? SetExtentUptodate(extent) :
			     ClearExtentDirty(extent);
		extent->disk = metadata_alloc(hc);
		extent_to_disk(extent);

		/* Restore flags. */
		extent->io.flags = flags;

		/* Take out a references for the metadata IOs. */
		endio_get(extent);
	}

	/* Take out a references for the extent data IOs. */
	endio_get(extent);
	SetExtentCopyActive(extent);

	/* REMOVEME: statistics. */
	atomic_inc(hc->stats.extent_data_io + (rw == WRITE));
	BUG_ON(extent_data_io(hc, extent, rw));

	if (CachePersistent(hc))
		BUG_ON(extent_meta_io(hc, extent, WRITE));
}

/* Process bios on an uptodate extent. */
static void bios_io(struct hstore_c *hc, struct extent *extent)
{
	int meta_io = 1, writes = 0;
	struct bio *bio;
	struct bio_list *bl = &extent->io.in;

	/* Loop through bio_list first, taking out the endio references. */
	bio_list_for_each(bio, bl) {
		int write = bio_data_dir(bio) == WRITE;

		writes += write;
		endio_get(extent);	/* Take an io reference per bio. */
		bio_remap(extent, bio);	/* Remap to cache or orig device. */

		/* REMOVEME: statistics. */
		atomic_inc(hc->stats.submitted_io + write);
	}

	/* Flag bio IO active on this extent. */
	SetExtentBioActive(extent);

	/*
	 * If I've got writes here *and* the extent hasn't been
	 * dirtied in a previous run -> update metadata on disk.
	 */
	if (writes) {
		if (CacheWriteThrough(hc)) {
			/* REMOVEME: statistics */
			atomic_inc(&hc->stats.extent_clear_uptodate);

			/*
			 * Flag extent not uptodate, because we're
			 * writing through to the original device.
			 */
			if (!TestClearExtentUptodate(extent))
				/* Write metadata only on first change. */
				meta_io = 0;
		} else if (!TestSetExtentDirty(extent)) {
			/* REMOVEME: statistics */
			atomic_inc(&hc->stats.extent_dirty);
			extent_dirty_add(extent);
		}

		/* Update metadata of this extent on cache device. */
		SetExtentBioWrite(extent);

		if (CachePersistent(hc) && meta_io) {
			extent->disk = metadata_alloc(hc);
			extent_to_disk(extent);
			/* Take IO reference for metadata write. */
			endio_get(extent);
			BUG_ON(extent_meta_io(hc, extent, WRITE));
		}
	}

	/* Submit all bios hanging off the extents input list. */
	while ((bio = bio_list_pop(bl)))
		generic_make_request(bio);
}

/* Set extents to validated and put them onto the LRU list. */
static void extents_validate_init(struct hstore_c *hc,
				  struct extent *extent)
{
	LIST_HEAD(free);

	do {
		atomic_inc(&hc->extents.free);
		atomic_inc(&hc->extents.initialized);
		list_add_tail(&extent->lists.init_lru, &free);
	} while ((extent = extent_init_pop(hc)));

	list_splice(&free, &hc->lists.lru);
}

/* Validate extents. */
static void extent_validate(struct hstore_c *hc, struct extent *extent)
{
	/*
	 * Metadata read -> insert into hash if used, so
	 * that hash hits can start to happen in do_bios().
	 */
	/* Reset to enable access. */
	ClearExtentBioActive(extent);
	ClearExtentBioWrite(extent);
	ClearExtentCopyActive(extent);
	ClearExtentWritingFree(extent);

	if (ExtentError(extent))
		DMERR("extent=%llu metadata read error",
	      	      (unsigned long long) extent->addr.cache.offset);
	else if (!ExtentFree(extent)) {
		/* Insert into hash if used. */
		extent_insert(extent);

		/*
		 * Take any left behind extents in need
		 * of IO off LRU and on dirty/flush list.
		 */
		if (!ExtentUptodate(extent))
			extent_flush_add(extent);
		else if (ExtentDirty(extent)) {
			/* REMOVEME: statistics */
			atomic_inc(&hc->stats.extent_dirty);
			extent_dirty_add(extent);
		}
	}

	/* Put on LRU list if errored or uptodate and clean. */
	if ((ExtentUptodate(extent) && !ExtentDirty(extent)) ||
	    ExtentError(extent))
		extent_lru_add(extent);

	/*
	 * A free extent got written or read -> we can
	 * validate all other extents on the init list
	 * and put them upfront the LRU list.
	 */
	if (ExtentFree(extent))
		extents_validate_init(hc, extent);
	else
		atomic_inc(&hc->extents.initialized);

	/* All extents done. */
	if (atomic_read(&hc->extents.initialized) ==
	    atomic_read(&hc->extents.total)) {
		/* Flag done with extents initialization. */
		ClearCacheInitializing(hc);
		ClearCacheInitializingNew(hc);
		DMINFO("initialized %s, %u total/%u free extents",
		       hc->devs.cache.dev->name,
		       atomic_read(&hc->extents.total),
		       atomic_read(&hc->extents.free));
	}

	/*
	 * Flag initialization of extent done, so
	 * that another one can be read if necessary.
	 */
	ClearCacheInitializationActive(hc);
}

/* Handle all endios on extents. */
static void do_endios(struct hstore_c *hc)
{
	struct extent *extent;

	while ((extent = extent_endio_pop(hc))) {
		if (extent->disk) {
			/* Transfer metadata to CPU on read. */
			if (ExtentMetaRead(extent)) {
				int r;
				sector_t offset = extent->addr.cache.offset;

				/*
				 * Need to set flags again, because they're
				 * transient and got overwritten from disk.
				 */
				extent_to_core(extent);
				SetExtentMetaRead(extent);
				r = extent_check(extent, offset);
				if (r) {
					/* Restore in case of error. */
					extent->addr.cache.offset = offset;
					SetExtentError(extent);
				}
			}

			/* Free disk header structure. */
			metadata_free(hc, extent->disk);
			extent->disk = NULL;

			/* Extent validation during initialization. */
			if (unlikely(TestClearExtentMetaRead(extent))) {
				extent_validate(hc, extent);
				continue;
			}
		}

		/* From hereon, we've got bio writes or extent copies. */

		/* Adjust extent state, if this was an extent copy. */
		if (TestClearExtentCopyActive(extent)) {
			if (!TestSetExtentUptodate(extent))
				/* Extent read off origin. */
				BUG_ON(ExtentDirty(extent));
			else if (TestClearExtentDirty(extent)) {
				/* Extent written to origin. */
				BUG_ON(!ExtentUptodate(extent));
				ClearCacheDirtyFlushing(hc);

				/* REMOVEME: statistics. */
				atomic_dec(&hc->stats.extent_dirty);
			}
		} else if (TestClearExtentBioActive(extent)) {
			/* End IO any bios *after* the metadata got updated. */
			_bio_endio_list(&extent->io.endio,
					ExtentError(extent) ? -EIO : 0);
			ClearExtentBioWrite(extent);
		}

		/* Clear free extent metadata writes. */
		if (TestClearExtentWritingFree(extent))
			ClearCacheWritingFree(hc);
		else if (bio_list_empty(&extent->io.in)) {
			if (!ExtentDirty(extent))
				/* No bios and not dirty -> put on LRU list. */
				extent_lru_add(extent);
		} else
			/* There's bios pending -> put on flush list. */
			extent_flush_add(extent);
	}
}

/* Initialize any extents on init list or resize cache. */
static void do_extents_init(struct hstore_c *hc)
{
	if (CacheInitializationActive(hc) ||
	    !CacheInitializing(hc) ||
	    CacheSuspend(hc))
		return;
	else {
		int rw = CacheInitializingNew(hc) ? WRITE : READ;
		/* Get next extent to initialize. */
		struct extent *extent = extent_init_pop(hc);

		BUG_ON(!extent);
		extent->disk = metadata_alloc(hc);

		if (rw == WRITE) {
			ClearExtentMetaRead(extent);
			extent_to_disk(extent);
		} else
			SetExtentMetaRead(extent);

		/* Take endio reference out and initiate IO. */
		endio_get(extent);
		SetCacheInitializationActive(hc);
		BUG_ON(extent_meta_io(hc, extent, rw));
	}
}


/* Resize cache on ctr argument request. */
static int cache_resize(struct dm_target *ti, sector_t cache_size);
static void do_cache_resize(struct hstore_c *hc)
{
	/* FIXME: Don't shrink when cache busy! .*/
	if (!CacheInitializing(hc) && TestClearCacheResize(hc)) {
		int r = cache_resize(hc->ti, hc->params.cache_new_size);

		if (r)
			SetCacheResize(hc);
	}
}

/* Restore caller context and safe extent reference for hstore_end_io(). */
static void bio_restore_context(struct bio *bio, struct extent *extent)
{
	union map_info *map_context = bio->bi_private;

	bio->bi_private = map_context->ptr;
	map_context->ptr = extent;
}

/* Restore context and error bio. */
static void _bio_restore_and_error(struct bio *bio)
{
	bio_restore_context(bio, NULL);
	_bio_endio(bio, -EIO);
}

/*
 * Handle all incoming/deferred bios.
 *
 * The following extent states are handled here:
 *   o can't get extent from hash or LRU -> put bio off.
 *   o else merge bio into the bio queue of the extent and put extent
 *     onto flush list unless extent is active (do_endios() will
 *     put those on flush list).
 */
static void do_bios(struct hstore_c *hc)
{
	int need_to_error;
	struct bio *bio;
	struct bio_list ios;
	struct extent *extent;

	/* When invalidating the cache or handling a barrier -> wait. */
	if (CacheInvalidate(hc) ||
	    (CacheBarrier(hc) && cc_ios_inflight(hc)))
		return;

	ClearCacheBarrier(hc);

	/*
	 * In case the original device isn't writable
	 * and the cache is full, I got to error the IO
	 */
	need_to_error = !OrigWritable(hc) &&
			list_empty(&hc->lists.lru) &&
			!CacheInitializing(hc);

	/* Fetch any deferred bios. */
	bio_list_init(&ios);
	bio_list_merge(&ios, &hc->io.defer);
	bio_list_init(&hc->io.defer);

	/* Quickly add all new bios queued to the end of the work list. */
	spin_lock_irq(&hc->io.lock);
	bio_list_merge(&ios, &hc->io.in);
	bio_list_init(&hc->io.in);
	spin_unlock_irq(&hc->io.lock);

	/* Got to make sure, that this is the MRU extent still. */
	extent = hc->extent_mru;
	if (extent && !list_empty(&extent->lists.init_lru))
		extent = NULL;

	/* Work all deferred or new bios on work list. */
	while ((bio = bio_list_pop(&ios))) {
		/* Once the barrier is flagged, defer further IO. */
		if (CacheBarrier(hc)) {
			_bio_defer(hc, bio);
			continue;
		}

		/* Flag a barrier write. */
		if (bio_empty_barrier(bio))
			SetCacheBarrier(hc);

		/*
		 * Check single MRU extent cache entry valid.
		 * On miss, try getting an extent from hash or LRU list.
		 */
		extent = extent_valid(extent, bio);
		if (!extent)
			extent = extent_get(hc, bio);

		if (extent)
			/* REMOVEME: statistics */
			atomic_inc(&hc->stats.hits);

		/* Can't get one -> put IO off (or error it; see below). */
		else {
			/* REMOVEME: statistics */
			atomic_inc(&hc->stats.misses);

			/*
			 * If we run out of LRU extents but can write
			 * to the original we defer, otherwise we need
			 * to error the IO (restoring private bio context
			 * before) unless we're initializing still.
			 */
			if (need_to_error)
				_bio_restore_and_error(bio);
			else
				_bio_defer(hc, bio);

			continue;
		}

		/*
		 * If extent is errored, error bio here,
		 * restoring bio private context before.
		 */
		if (ExtentError(extent)) {
			_bio_restore_and_error(bio);
			continue;
		}

		/*
		 * Put bio on extents input queue,
		 * restoring bio private context before.
		 */
		bio_restore_context(bio, extent);
		bio_list_add(&extent->io.in, bio);

		/*
		 * FIXME: add when extent is active ?
		 *
		 * Need to decide if we can access the extent (ie. bios active)
		 * or not (ie. write bios and extent being written back).
		 *
		 */
		if (ExtentBioActive(extent) || ExtentCopyActive(extent))
			/* REMOVEME: statistics */
			atomic_inc(&hc->stats.extent_copy_active);
		else
			/* Put extent on flush list for IO. */
			extent_flush_add(extent);
	}

	/* Preserve MRU extent reference. */
	hc->extent_mru = extent;
}

/*
 * Check, if extents get completely written over and if so,
 * set them uptodate in order to suppress superfluous reads
 * from slow original device, hence raising performance.
 */
static void do_overwrite_check(struct hstore_c *hc)
{
	struct extent *extent;

	list_for_each_entry(extent, &hc->lists.flush, lists.endio_flush) {
		/* Skip any uptodate extents. */
		if (!ExtentUptodate(extent) &&
		    !ExtentCopyActive(extent)) {
			unsigned sectors = 0;
			struct bio *bio;

			/* Sum up all write bio sectors. */
			bio_list_for_each(bio, &extent->io.in) {
				if (bio_data_dir(bio) == WRITE)
					sectors += bio_sectors(bio);
			}

			/* Set extent uptodate in case writes cover it fully. */
			if (sectors == hc->extents.size) {
				SetExtentUptodate(extent);

				/* REMOVEME: statistics. */
				atomic_inc(&hc->stats.overwrite);
			}
		}
	}
}

/* Flush out any dirty extents one by one. */
static void do_dirty(struct hstore_c *hc)
{
	/*
	 * Only put any dirty extent on flush list in case:
	 *   o we're not asked to suspend
	 *   o the original device is writable
	 *   o we're not flushing out dirty extents right now
	 *   o we're idle (ie. no bios pending)
	 *   -or-
	 *   o the LRU list is empty
	 */
	if (!CacheSuspend(hc) &&
	    OrigWritable(hc) &&
	    !CacheDirtyFlushing(hc) &&
	    (!cc_ios_inflight(hc) || !atomic_read(&hc->extents.free))) {
		struct extent *extent, *tmp;

		list_for_each_entry_safe(extent, tmp, &hc->lists.dirty,
					 lists.dirty) {
			/* Don't initiate IO on active extent. */
			if (ExtentBioActive(extent) ||
			    !bio_list_empty(&extent->io.in))
				continue;

			/* Flag that we're flushing a dirty extent. */
			SetCacheDirtyFlushing(hc);

			/* Take extent off dirty list and add to flush list. */
			list_del_init(&extent->lists.dirty);
			extent_flush_add(extent);
			break;
		}
	}
}

/* Walk the list of extents on flush list and initiate io. */
static void do_flush(struct hstore_c *hc)
{
	struct extent *extent;

	/* Work all extents on flush list. */
	while ((extent = extent_flush_pop(hc))) {
		if (ExtentUptodate(extent)) {
			/*
			 * Extent is uptodate; we can write it
			 * out to the origin or submit bios.
			 */
			if (bio_list_empty(&extent->io.in)) {
				BUG_ON(!ExtentDirty(extent));
				/* Write extent out to the origin. */
				extent_io(hc, extent, WRITE);
			} else
				/* Submit any bios hanging off this extent. */
				bios_io(hc, extent);
		} else {
			BUG_ON(ExtentDirty(extent));
			/* If the extent isn't uptodate -> read it in. */
			extent_io(hc, extent, READ);
		}
	}
}

/* Wake up any waiters in case we're idle. */
static void do_wake(struct hstore_c *hc)
{
	/* Wake up any suspend waiter. */
	if (cc_idle(hc))
		wake_up(&hc->io.suspendq);

	/* Wake up any cache invalidation waiter. */
	if (cc_cache_clean(hc))
		wake_up(&hc->io.invalidateq);
}

/*
 * Worker thread.
 *
 * o do setting changes requested via message interface.
 * o handle all outstanding endios on extents.
 * o initialize any uninitialized extents.
 * o resize cache if requested by constructor/message interface.
 * o work on all new queued bios putting them on extent bio queues
 *   writing next free extent metadata header if necessary.
 * o check for extents, which get completely written over and
 *   avoid extent reads if not uptodate.
 * o add any dirty extents to flush list if idle.
 * o submit IO for extents on flush list.
 * o wake any suspend or cache invalidation waiters if idle.
 */
static void do_hstore(struct work_struct *ws)
{
	struct hstore_c *hc = container_of(ws, struct hstore_c, io.ws);

	if (TestSetCacheThreadRunning(hc))
		DMERR("%s Recalled!!!", __func__);
	else {
		do_settings(hc);
		do_endios(hc);
		do_extents_init(hc);
		do_cache_resize(hc);
		do_bios(hc);
		do_overwrite_check(hc);
		do_dirty(hc);
		do_flush(hc);
		do_wake(hc);
		ClearCacheThreadRunning(hc);
	}
}

/* Free a list of cache extents structures. */
static void extents_free_list(struct list_head *list)
{
	struct list_head *pos, *tmp;

	list_for_each_safe(pos, tmp, list) {
		list_del(pos);
		extent_free(extent_pos(pos));
	}
}

/* Free the LRU list. */
static void hstore_extents_free(struct hstore_c *hc)
{
	extents_free_list(&hc->lists.init);
	extents_free_list(&hc->lists.lru);
}

/* Initialize cache extent structures in memory and add them to init list. */
static int cache_extents_alloc(struct hstore_c *hc, sector_t cache_size,
			       sector_t start, unsigned count, gfp_t flags)
{
	sector_t increment = extent_size(hc), offset = start,
		 end = start + count * increment;
	struct extent *extent;

	for (; offset < end; offset += increment) {
		extent = extent_alloc(hc, flags);
		if (!extent)
			return -ENOMEM;

		/* All extents go to init list for later processing. */
		extent_init(hc, extent, offset);
		extent_init_add(extent);
	}

	return 0;
}

/*
 * Create or read the cache device header.
 */
static int cache_header_init(struct hstore_c *hc, enum handle_type handle)
{
	int r;

	hc->disk_header = metadata_alloc(hc);
	r = header_io(hc, READ);
	if (r)
		DM_ERR_RET(r, "reading cache header from %s",
			   hc->devs.cache.dev->name);

	header_to_cpu(hc);

	/* Found disk header magic but invalid metadata -> WARN and bail out. */
	r = header_check(hc);
	if (r == -EPERM)
		DMWARN("header magic found but header data invalid "
		       "(hstore metadata version invalid?)");

	if (handle == CREATE_CACHE || (handle == AUTO_CACHE && r)) {
		if (handle == AUTO_CACHE && hc->params.params < 2) {
			DMERR("need cache size with auto to initialize");
			r = -EINVAL;
			goto err;
		}

		DMINFO("%sriting cache device %s header",
		       r ? "w" : "overw", hc->devs.cache.dev->name);
		r = header_write(hc);
		if (r) {
			DMERR("writing cache header to %s",
			      hc->devs.cache.dev->name);
			goto err;
		}

		/* Flag extent initialization writes. */
		SetCacheInitializingNew(hc);
	} else {
		if (r)
			goto err;

		DMINFO("read cache device %s header",
		       hc->devs.cache.dev->name);
		hc->extents.size = hc->disk_header->extent_size;
		hc->devs.cache.size = hc->disk_header->dev_size;
		hc->io.flags |= hc->disk_header->flags;

		/* Flag extent initialization reads. */
		ClearCacheInitializingNew(hc);
	}

	/* Set masks for fast bio -> extent mapping. */
	hc->extent_mask_inv = hc->extents.size - 1;
	hc->extent_mask = ~hc->extent_mask_inv;

err:
	metadata_free(hc, hc->disk_header);
	hc->disk_header = NULL;
	return r;
}

/*
 * Destruct a cache mapping.
 */
static void hstore_dtr(struct dm_target *ti)
{
	struct hstore_c *hc = ti->private;

	flush_workqueue(_hstore_wq);

	hstore_extents_free(hc);
	hash_exit(&hc->hash);

	if (hc->io.kcopyd_client)
		dm_kcopyd_client_destroy(hc->io.kcopyd_client);

	if (hc->io.dm_io_client)
		dm_io_client_destroy(hc->io.dm_io_client);

	if (hc->io.metadata_pool)
		mempool_destroy(hc->io.metadata_pool);

	if (hc->devs.orig.dev)
		dm_put_device(ti, hc->devs.orig.dev);

	if (hc->devs.cache.dev)
		dm_put_device(ti, hc->devs.cache.dev);

	kfree(hc);
}

/* Constructor <dev_path> <offset> helper */
static int get_dev(struct dm_target *ti, char **argv, struct c_dev *dev)
{
	int r;
	unsigned long long tmp;

	r = sscanf(argv[1], "%llu", &tmp);
	if (r != 1)
		TI_ERR("Invalid device start sector");

	dev->start = tmp;
	if (dev->start >= dev->size - MIN_CACHE_SIZE)
		TI_ERR("Invalid device start/length");

	r = dm_get_device(ti, argv[0], dev->start, dev->size,
			  dev->mode, &dev->dev);
	if (r)
		TI_ERR("Device lookup failed");

	return 0;
}

/* Check helper: device sizes make sense? */
static int size_check(struct hstore_c *hc)
{
	sector_t cache_len, orig_len;
	struct devs *d = &hc->devs;

	orig_len = d->orig.size - d->orig.start;
	cache_len = d->cache.size - d->cache.start;
	if (orig_len < cache_len)
		DM_ERR("origin device smaller than cache device");

	if (!multiple(d->cache.start, META_SECTORS))
		DM_ERR("cache offset is not divisable by %llu",
		       (unsigned long long) META_SECTORS);

	if (extents_total(hc) < EXTENTS_MIN)
		DM_ERR("cache too small for extent size");

	return 0;
}

/* Resize cache device. */
static int cache_resize(struct dm_target *ti, sector_t cache_size)
{
	int grow, r;
	unsigned count, free = 0;
	sector_t extents, extents_old, size_old;
	struct hstore_c *hc = ti->private;
	struct c_dev *cache = &hc->devs.cache;
	struct dm_dev *dev;
	struct extent *extent;
	struct list_head list, *pos = &hc->lists.lru;

	if (!cache_size)
		return 0;

	/* Calculate absolute number of extents fitting cache device size. */
	extents = cache_size - extents_start(hc);
	do_div(extents, extent_size(hc));

	extents_old = extents_total(hc);
	if (extents < EXTENTS_MIN) {
		DMERR("cache size too small");
		goto err;
	} else if (extents < extents_old)
		grow = 0;
	else if (extents > extents_old)
		grow = 1;
	else {
		DMERR("cache size wouldn't change");
		goto err;
	}

	/* Try aquiring new cache device size. */
	/* FIXME: limit checks bogus in dm_get_device()! */
	r = dm_get_device(ti, cache->dev->name, cache->start,
			  cache_size, cache->mode, &dev);
	if (r) {
		DM_ERR("device size %llu invalid",
		       (unsigned long long) cache_size);
	} else
		dm_put_device(ti, dev);	/* Release device reference. */

	/* Grow: try allocating additional extent structures. */
	if (grow) {
		sector_t start = extents_start(hc) +
				 extents_old * extent_size(hc);
		sector_t tmp = cache_size - start;
		do_div(tmp, extent_size(hc));
		count = tmp;
		r = cache_extents_alloc(hc, cache_size, start, count,
					GFP_NOIO);
		if (r) {
			DMERR("can't allocate requested %u extents for %s",
			      count, cache->dev->name);
			goto err_free;
		}
	} else
		count = extents_old - extents;

	/* Find position for insertion/deletion in LRU list. */
	spin_lock_irq(&hc->lists.locks.lru);
	list_for_each_entry(extent, &hc->lists.lru, lists.init_lru) {
		if (ExtentFree(extent)) {
			if (++free == count)
				break;

			pos = pos->next;
		} else
			break;
	}

	if (grow) {
		/* Insert list of newly allocated extents into LRU list. */
		list_splice_init(&hc->lists.init, pos);
		INIT_LIST_HEAD(&list);

		/* Adjust extent counters. */
		atomic_add(count, &hc->extents.total);
		atomic_add(count, &hc->extents.free);
		atomic_add(count, &hc->extents.initialized);
	} else if (free == count) {
		unsigned i = count;

		/* Move extent to free below off the lru list. */
		INIT_LIST_HEAD(&list);
		while (i--)
			list_move_tail(hc->lists.lru.next, &list);

		/* Adjust extent counters. */
		atomic_sub(count, &hc->extents.total);
		atomic_sub(count, &hc->extents.free);
		atomic_sub(count, &hc->extents.initialized);
	}
	spin_unlock_irq(&hc->lists.locks.lru);

	/* Enough free extents to shrink ? */
	if (!grow && free != count) {
		DMERR("not enough free extents to shrink (%u/%u)", free,
		      count);
		DMINFO("could shrink to %llu sectors", (unsigned long long)
		       hc->devs.cache.size - free * extent_size(hc));
		goto err;
	}

	/* Set new cache size. */
	size_old = hc->devs.cache.size;
	hc->devs.cache.size = cache_size;

	/* Update disk header with new cache size. */
	hc->disk_header = metadata_alloc(hc);
	r = header_write(hc);
	metadata_free(hc, hc->disk_header);
	if (r) {
		DMERR("FATAL: Error writing cache header to %s",
		      cache->dev->name);
		hc->devs.cache.size = size_old;
		goto err_free;
	}

	/* Free any extents we shrunk the cache by. */
	if (!grow)
		extents_free_list(&list);

	/* Switch/drop reference. */
	DMINFO("%s %s to %llu sectors",
	       grow ? "Grown" : "Shrunk", cache->dev->name,
	       (unsigned long long) cache_size);
	return 0;

err_free:
	/* Free any allocated extents before the allocation failure. */
	extents_free_list(&hc->lists.init);
err:
	return -EINVAL;
}

/* Return string for cache open handle. */
static const char *handle_str(enum handle_type handle)
{
	static const char *handle_to_str[] = {
		"auto",
		"create",
		"open",
	};

	return handle_to_str[handle];
}

/*
 * Check, if @str is listed on variable (const char *) list of strings.
 *
 * Returns 1 for found on list and 0 for failure.
 */
static int
str_listed(const char *str, ...)
{
	int r = 0, c = 1;
	const char *s;
	va_list str_list;

	va_start(str_list, str);

	while ((s = va_arg(str_list, const char *))) {
		if (!strncmp(str, s, strlen(str))) {
			r = c;
			break;
		}

		c++;
	}

	va_end(str_list);
	return r;
}

/* Check origin device read/write parameter */
static int orig_rw(const char *arg)
{
	if (str_listed(arg, "ro", "readonly"))
		return READ;
	else if (str_listed(arg, "default", "rw", "readwrite"))
		return WRITE;
	else
		return -EINVAL;
}

enum write_type { WRITEBACK, WRITETHROUGH };
static enum write_type cache_write_policy(const char *arg)
{
	if (str_listed(arg, "default", "wb", "writeback"))
		return WRITEBACK;
	else if (str_listed(arg, "wt", "writethrough"))
		return WRITETHROUGH;
	else
		return -EINVAL;
}

enum store_type { INVALIDATE, TRANSIENT, PERSISTENT };
static enum store_type cache_store_policy(const char *arg)
{
	if (str_listed(arg, "default", "persistent"))
		return PERSISTENT;
	else if (str_listed(arg, "transient"))
		return TRANSIENT;
	else if (str_listed(arg, "invalidate"))
		return INVALIDATE;
	else
		return -EINVAL;
}


/*
 * Construct a cache mapping:
 *
 * FIXME:
 *   o allow extent recovery in case header's lost.
 *
 * cache_dev_name cache_dev_start
 * #varibale_params <params> orig_dev_name orig_dev_start
 *
 * #variable_params = 0-6
 *
 * params = {auto/create/open} [cache_dev_size [#cache_extent_size \
 *	    [default/rw/readwrite/ro/readonly \
 *	     [default/wb/writeback/wt/writethrough \
 *           [default/persistent/transient/invalidate]]]]]
 *
 * 'auto' causes open of a cache with a valid header or
 * creation of a new cache if there's no vaild one with
 * cache_dev_size in case the header's invalid. If 'auto'
 * is being used on a non-existing cache without cache_dev_size
 * or cache_dev_size = 0, the constructor fails
 *
 * 'create' enforces creation of a new cache with cache_dev_size.
 * WARNING: this overwrites an existing cache!!!
 *
 * 'open' requires a valid cache to exist. No new one will be
 * created ever.
 *
 * #variable_params:
 * 0: the cache device must exist and will be opened or the constructor fails
 * 1 + 'open': equal to '0'
 * 1 + 'auto': equal to '0'
 * 2 + 'open': the cache device must be initialized and resizing will
 *	       get tried to cache_dev_size. cache_dev_size = 0 doesn't
 *	       change the cache size.
 * 2 + 'create': the cache device will get initialized and sized
 *	         to cache_dev_size.
 * 2 + 'auto': the cache device will either be opend and tried to resize
 * 	       or get initialized and sized to cache_dev_size.
 * 3: on create (either implicit or via 'auto'),
 *    this cache:extens_size  will be used.
 * 4: the origin device will be opend read only/read write,
 *    defaulting to read write
 * 5: the cache will be set to writeback or writethrough,
 *    defaulting to writeback
 * 6: the cache will be set to  persistency or transient, defaulting
 *    to persistent *or* a persistent cache will get purged
 */
#define MIN_PARAMS	5
#define MAX_PARAMS	(MIN_PARAMS + 6)
static int hstore_ctr(struct dm_target *ti, unsigned argc, char **argv)
{
	/* Default to writable original device. */
	int hstore_params, r, orig_access = WRITE;
	unsigned num_pages = 16; /* 16 pages for metadata IO in parallel. */
	unsigned long long tmp;
	sector_t cache_size = 0, cache_start, extents,
		 extent_size = EXTENT_SECTORS_DEFAULT;
	/* Defaults: persitent, writeback cache and open. */
	enum store_type store_policy = PERSISTENT;
	enum write_type write_policy = WRITEBACK;
	enum handle_type handle = OPEN_CACHE;
	struct hstore_c *hc;

	if (!range_ok(argc, MIN_PARAMS, MAX_PARAMS))
		TI_ERR("Invalid argument count");

	/* Get cache device offset. */
	if (sscanf(argv[1], "%llu", &tmp) != 1 ||
	    (tmp && (!is_power_of_2(tmp) || tmp < META_SECTORS)))
		TI_ERR("Invalid cache device start argument");

	cache_start = tmp;

	/* Get #hstore_params. */
	if (sscanf(argv[2], "%d", &hstore_params) != 1 ||
	    !range_ok(hstore_params, 0, 6))
		TI_ERR("Invalid hstore parameter number argument");

	if (argc - hstore_params - 3 != 2)
		TI_ERR("Invalid number of original device arguments");

	/* Handle any variable cache parameters. */
	if (hstore_params) {
		if (str_listed(argv[3], handle_str(CREATE_CACHE), NULL)) {
			handle = CREATE_CACHE;
			if (hstore_params == 1)
				TI_ERR("hstore create needs another argument");
		} else if (str_listed(argv[3], handle_str(OPEN_CACHE), NULL)) {
			handle = OPEN_CACHE;
			if (hstore_params > 5)
				TI_ERR("Too many arguments with hstore 'open'");
		} else if (str_listed(argv[3], handle_str(AUTO_CACHE), NULL))
			handle = AUTO_CACHE;
		else
			TI_ERR("Invalid hstore auto/create/open argument");

		/* Get cache device size. */
		if (hstore_params > 1) {
			if (sscanf(argv[4], "%llu", &tmp) != 1 ||
			    tmp < MIN_CACHE_SIZE)
				TI_ERR("Invalid cache device size argument");

			cache_size = tmp;
		}

		/* Get origin rw mode. */
		if (hstore_params > 2) {
			orig_access = orig_rw(argv[5]);
			if (orig_access < 0)
				TI_ERR("Invalid original rw parameter");
		}

		/* Get cache extent size. */
		if (hstore_params > 3) {
			if (sscanf(argv[6], "%llu", &tmp) != 1 ||
			    !is_power_of_2(tmp) ||
			    (tmp && (tmp < META_SECTORS)))
				TI_ERR("Invalid cache extent size argument")

			extent_size = tmp;
		}

		/* Get cache write policy. */
		if (hstore_params > 4) {
			write_policy = cache_write_policy(argv[7]);
			if (write_policy < 0)
				TI_ERR("Invalid cache write policy argument");
		}

		/* Get cache persistency policy / invalidation request. */
		if (hstore_params > 5) {
			store_policy = cache_store_policy(argv[8]);
			if (store_policy < 0)
				TI_ERR("Invalid cache persistency/invalidation "
				       "policy argument");
		}
	}

	/* Got all constructor information to allocate context. */
	hc = kzalloc(sizeof(*hc), GFP_KERNEL);
	if (!hc)
		TI_ERR_RET("Cannot allocate hstore context", -ENOMEM);

	/* Save #hstore_params and hstore_params for status output. */
	hc->params.params = hstore_params;
	hc->params.handle = handle;
	hc->params.cache_size = cache_size;
	hc->params.extent_size = extent_size;
	hc->params.write_policy = write_policy;
	hc->params.store_policy = store_policy;

	/* Preset extent size. */
	hc->extents.size = extent_size ? extent_size :
					    EXTENT_SECTORS_DEFAULT;

	init_waitqueue_head(&hc->io.suspendq);	/* Suspend waiters. */
	init_waitqueue_head(&hc->io.invalidateq);/* Invalidate waiters. */
	atomic_set(&hc->io.ref, 0);
	atomic_set(&hc->extents.free, 0);
	atomic_set(&hc->extents.initialized, 0);
	bio_list_init(&hc->io.defer);
	bio_list_init(&hc->io.in);
	spin_lock_init(&hc->io.lock);
	spin_lock_init(&hc->lists.locks.endio);
	spin_lock_init(&hc->lists.locks.lru);
	INIT_LIST_HEAD(&hc->lists.dirty);
	INIT_LIST_HEAD(&hc->lists.endio);
	INIT_LIST_HEAD(&hc->lists.flush);
	INIT_LIST_HEAD(&hc->lists.init);
	INIT_LIST_HEAD(&hc->lists.lru);
	stats_init(&hc->stats);
	ti->private = hc;
	hc->ti = ti;

	/* Set cache and original read/write access modes. */
	hc->devs.cache.mode = hc->devs.orig.mode = FMODE_READ | FMODE_WRITE;
	if (orig_access == READ)
		hc->devs.orig.mode = FMODE_READ;

	/* Get cache device. */
	hc->devs.cache.size = cache_size ? cache_size :
					   cache_start + META_SECTORS;;
	r = get_dev(ti, argv, &hc->devs.cache);
	if (r) {
		ti->error = "Cannot access cache device";
		goto err;
	}

	/* Get original (cached) device. */
	hc->devs.orig.size = ti->len;
	r = get_dev(ti, argv + hstore_params + 3, &hc->devs.orig);
	if (r) {
		ti->error = "Cannot access origin device";
		goto err;
	}

	/* Create mempool for disk headers. */
	hc->io.metadata_pool =
		mempool_create_kmalloc_pool(num_pages, SECTOR_SIZE);
	if (!hc->io.metadata_pool) {
		ti->error = "Failure allocating memory pool";
		r = -ENOMEM;
		goto err;
	}

	/* Use dm_io to io cache metadata. */
	hc->io.dm_io_client = dm_io_client_create(num_pages);
	if (IS_ERR(hc->io.dm_io_client)) {
		r = PTR_ERR(hc->io.dm_io_client);
		goto err;
	}

	/*
	 * Initialize cache device header.
	 *
	 * Must be done first, because we need the extent size
	 * in an existing header for kcopyd resource calculation.
	 */
	r = cache_header_init(hc, handle);
	if (r)
		goto err;

	if (store_policy == INVALIDATE) {
		DMWARN("Ignoring \"invalidate\" argument");
		store_policy = PERSISTENT;
	}

	/* Change cache flags if requested by ctr arguments. */
	if (store_policy == PERSISTENT)
		SetCachePersistent(hc);
	else if (store_policy == TRANSIENT)
		ClearCachePersistent(hc);

	if (write_policy == WRITEBACK)
		ClearCacheWriteThrough(hc);
	else if (write_policy == WRITETHROUGH)
		SetCacheWriteThrough(hc);

	hc->disk_header = metadata_alloc(hc);
	r = header_write(hc);
	if (r) {
		metadata_free(hc, hc->disk_header);
		ti->error = "Error writing cache device header";
		goto err;
	}

	metadata_free(hc, hc->disk_header);

	/* Check, if device sizes are valid. */
	r = size_check(hc);
	if (r)
		goto err;

	/*
	 * Try reaquiring the cache device when size
	 * in header differs from ctr parameter.
	 */
	if (cache_size && cache_size != hc->devs.cache.size) {
		dm_put_device(ti, hc->devs.cache.dev);
		r = get_dev(ti, argv, &hc->devs.cache);
		if (r)
			goto err;

		/* Flag for worker thread, that cache needs resizing. */
		hc->params.cache_new_size = cache_size;
		SetCacheResize(hc);
	}

	/* Calculate amount of kcopyd pages needed. */
	extent_size = hc->extents.size;
	do_div(extent_size, to_sector(PAGE_SIZE));
	num_pages *= extent_size;

	/* I use kcopyd to IO data to/from the cache. */
	r = dm_kcopyd_client_create(num_pages, &hc->io.kcopyd_client);
	if (r)
		goto err;

	/* Initialize IO hash */
	r = hash_init(hc);
	if (r) {
		DMERR("couldn't create extent hash");
		goto err;
	}

	/* Allocate extent structs and put the on init list. */
	extents = extents_total(hc);
	atomic_set(&hc->extents.total, extents);
	r = cache_extents_alloc(hc, hc->devs.cache.size,
				extents_start(hc), extents, GFP_KERNEL);
	if (r)
		goto err;

	/* No larger bios than the extent size and no boundary crossing. */
	ti->split_io = hc->extents.size;

	/* Flag extent initialization needs doing by worker thread. */
	if (CachePersistent(hc))
		SetCacheInitializing(hc);
	else
		/* Else do it here. */
		extents_validate_init(hc, extent_init_pop(hc));

	INIT_WORK(&hc->io.ws, do_hstore);
	return 0;

err:
	hstore_dtr(ti);

	if (r == -ENOMEM)
		TI_ERR_RET("Out of memory", r);

	return r;
}

/* Queues bios to the cache and wakes up worker thread. */
static inline void queue_bio(struct hstore_c *hc, struct bio *bio)
{
	spin_lock_irq(&hc->io.lock);
	bio_list_add(&hc->io.in, bio);
	spin_unlock_irq(&hc->io.lock);

	/* REMOVEME: statistics */
	atomic_inc(hc->stats.io + (bio_data_dir(bio) == WRITE));

	/* Wakeup worker to deal with bio input list. */
	wake_do_hstore(hc);
}

/*
 * Map a cache io by handling it in the worker thread.
 */
static int hstore_map(struct dm_target *ti, struct bio *bio,
		      union map_info *map_context)
{
	/* I don't want to waste cache capacity. */
	if (bio_rw(bio) == READA)
		return -EIO;

	/*
	 * Save caller private context in map_context->ptr
	 * and safe map_context reference for main thread in
	 * bio->bi_private (for restore in worker) in order
	 * to be able to use it to squirrel an extent
	 * reference from do_bios() to hstore_end_io().
	 */
	map_context->ptr = bio->bi_private;
	bio->bi_private = map_context;

	bio->bi_sector -= ti->begin;	/* Remap sector to target begin. */
	queue_bio(ti->private, bio);	/* Queue bio to the worker. */
	return DM_MAPIO_SUBMITTED;	/* Handle later. */
}

/* End io method. */
static int hstore_end_io(struct dm_target *ti, struct bio *bio,
			 int error, union map_info *map_context)
{
	struct extent *extent = map_context->ptr;

	if (extent) {
		/* We've got a bio IO error and flag that on the extent. */
		if (unlikely(error))
			SetExtentError(extent);

		/*
		 * I want another shot on this extents bios in order to
		 * make sure, that the metadata got updated in case of
		 * writes before I report endios up.
		 *
		 * In case of read bios only, where no metadata
		 * update happens, I optimize this out.
		 */
		if (ExtentBioWrite(extent)) {
			unsigned long flags;

			/* Make sure, we don't loop infinitely. */
			map_context->ptr = NULL;

			/*
			 * Need a spinlock here, because endios
			 * can be processed in parallel.
			 */
			spin_lock_irqsave(&extent->io.endio_lock, flags);
			bio_list_add(&extent->io.endio, bio);
			spin_unlock_irqrestore(&extent->io.endio_lock, flags);

			endio_put(extent);
			return DM_ENDIO_INCOMPLETE;	/* Another shot. */
		} else
			/* For reads, just drop the reference. */
			endio_put(extent);
	}

	return 0;
}

/* Flush method. */
static void hstore_flush(struct dm_target *ti)
{
	struct hstore_c *hc = ti->private;

	flush_workqueue(_hstore_wq);

	/* Wait until all io has been processed. */
	wait_event(hc->io.suspendq, cc_idle(hc));
}

/* Post suspend method. */
static void hstore_presuspend(struct dm_target *ti)
{
	/* Tell worker thread to stop initiationg new IO. */
	SetCacheSuspend((struct hstore_c *) ti->private);
	hstore_flush(ti);
}

/* Resume method */
static void hstore_resume(struct dm_target *ti)
{
	struct hstore_c *hc = ti->private;

	/* Tell worker thread that it may initiate new IO. */
	ClearCacheSuspend(hc);

	/*
	 * Wakeup worker to kick off IO processing in case
	 * of any left behind extents at device suspension.
	 */
/* FIXME: let worker always run?
	if (!list_empty(&hc->lists.dirty) ||
	    !list_empty(&hc->lists.init) || !list_empty(&hc->lists.flush))
*/
		wake_do_hstore(hc);
}

/* Message handler for origin readonly <-> readwrite transitions. */
static int msg_access(struct hstore_c *hc, char *arg)
{
	int r = orig_rw(arg);

	if (r < 0) {
		DMWARN("unrecognised cache access argument received.");
		return r;
	}

	if (r == WRITE) {
		if (OrigWritable(hc)) {
			DMWARN("origin is already readwrite.");
			return -EPERM;
		}
	} else if (!OrigWritable(hc)) {
		DMWARN("origin is already readonly.");
		return -EPERM;
	}

	/* Flag worker thread has to adjust access mode. */
	SetCacheChangeRW(hc);

	/*
	 * Wakeup worker to change access and kick off any dirty
	 * extents processing in case we switch origin from read
	 * to write access.
	 */
	wake_do_hstore(hc);
	return 0;
}

/* Invalidate cache. Cache needs to be quiesced (ie. suspended). */
static void cache_invalidate(struct hstore_c *hc)
{
	sector_t increment = extent_size(hc), offset = extents_start(hc);
	struct extent *extent;

	list_for_each_entry(extent, &hc->lists.lru, lists.init_lru) {
		if (!ExtentFree(extent))
			atomic_inc(&hc->extents.free);

		extent_hash_del(extent);
		extent_init(hc, extent, offset);
		offset += increment;
	}
}

/* Message handler to change cache persistency setting or invalidate cache. */
static int msg_cache(struct hstore_c *hc, char *arg)
{
	int r = cache_store_policy(arg);

	if (r < 0)
		DM_ERR("invalid cache persistency argument.");

	/* Wait for initialization to finish. */
	if (CacheInitializing(hc))
		return -EPERM;

	if (r == PERSISTENT) {
		if (CachePersistent(hc))
			return -EPERM;
	} else if (r == TRANSIENT) {
		if (!CachePersistent(hc))
			return -EPERM;
	} else if (r == INVALIDATE) {
		SetCacheInvalidate(hc);
		wait_event(hc->io.invalidateq, cc_cache_clean(hc));
		cache_invalidate(hc);

		if (CachePersistent(hc))
			write_free_header(hc);

		ClearCacheInvalidate(hc);
		goto out;
	} else
		DM_ERR("invalid cache persistency argument.");

	wake_do_hstore(hc);
out:
	return 0;
}

/* Message handler to resize cache device. */
static int msg_resize(struct hstore_c *hc, char *arg)
{
	unsigned long long tmp;

	/* Wait for initialization or resizing to finish. */
	if (CacheInitializing(hc) ||
	    CacheResize(hc))
		return -EPERM;

	if (sscanf(arg, "%llu", &tmp) != 1 ||
	    tmp < extents_start(hc) + EXTENTS_MIN * extent_size(hc))
		return -EINVAL;

	hc->params.cache_new_size = tmp;
	SetCacheResize(hc);
	wake_do_hstore(hc);
	return 0;
}

/* Message handler to change cache write policy. */
static int msg_write_policy(struct hstore_c *hc, char *arg)
{
	int r = cache_write_policy(arg);

	if (r < 0)
		DM_ERR("invalid cache write policy argument.");

	if (r == WRITEBACK) {
		if (!CacheWriteThrough(hc))
			return -EINVAL;
	} else if (CacheWriteThrough(hc))
		return -EINVAL;

	/* Flag worker thread: has to change cache write policy. */
	SetCacheChangeWritePolicy(hc);
	wake_do_hstore(hc);
	return 0;
}

/* Message method. */
static int hstore_message(struct dm_target *ti, unsigned argc, char **argv)
{
	if (argc == 1) {
		if (str_listed(argv[0], "flush")) {
			struct hstore_c *hc = ti->private;

			if (!CacheSuspend(hc)) {
				hstore_flush(ti);
				return 0;
			} else
				DM_ERR_RET(-EPERM, "Cache suspended");
		}
	} else if (argc == 2) {
		if (str_listed(argv[0], "access"))
			return msg_access(ti->private, argv[1]);
		else if (str_listed(argv[0], "cache"))
			return msg_cache(ti->private, argv[1]);
		else if (str_listed(argv[0], "resize"))
			return msg_resize(ti->private, argv[1]);
		else if (str_listed(argv[0], "write_policy"))
			return msg_write_policy(ti->private, argv[1]);
	}

	DMWARN("Unrecognised cache message received.");
	return -EINVAL;
}

/* Status output method. */
static int hstore_status(struct dm_target *ti, status_type_t type,
			 char *result, unsigned maxlen)
{
	unsigned dirty, extent_valid, handle, hits;
	ssize_t sz = 0;
	struct hstore_c *hc = ti->private;
	struct stats *s;

	switch (type) {
	case STATUSTYPE_INFO:
		s = &hc->stats;
		extent_valid = atomic_read(&s->extent_valid);
		dirty = atomic_read(&s->extent_dirty),
		    hits = atomic_read(&s->hits);
		DMEMIT
		    ("v=%s %s %s %s %s %s es=%llu flgs=%lu bkts=%u r=%u w=%u "
		     "rd=%u wd=%u rs=%u ws=%u ca=%u dr=%u dw=%u mr=%u mw=%u "
		     "ev=%u einv=%u ed=%u ec=%u h=%u m=%u ov=%u wf=%u wrf=%u "
		     "ef=%u ei=%u et=%u", version,
		     OrigWritable(hc) ? "rw" : "ro",
		     CacheInitializing(hc) ? "INIT" : "ready",
		     CachePersistent(hc) ? "pers" : "trans",
		     CacheWriteThrough(hc) ? "wt" : "wb",
		     dirty ? "DIRTY" : "clean",
		     (unsigned long long) hc->extents.size,
		     hc->io.flags, hc->hash.buckets,
		     atomic_read(s->io), atomic_read(s->io + 1),
		     atomic_read(s->deferred_io),
		     atomic_read(s->deferred_io + 1),
		     atomic_read(s->submitted_io),
		     atomic_read(s->submitted_io + 1),
		     atomic_read(&s->extent_copy_active),
		     atomic_read(s->extent_data_io),
		     atomic_read(s->extent_data_io + 1),
		     atomic_read(s->extent_meta_io),
		     atomic_read(s->extent_meta_io + 1), extent_valid,
		     hits - extent_valid, dirty,
		     atomic_read(&s->extent_clear_uptodate),
		     atomic_read(&s->hits), atomic_read(&s->misses),
		     atomic_read(&s->overwrite),
		     atomic_read(&s->write_free),
		     atomic_read(&s->writing_free),
		     atomic_read(&hc->extents.free),
		     atomic_read(&hc->extents.initialized),
		     atomic_read(&hc->extents.total));
		break;

	case STATUSTYPE_TABLE:
		handle = hc->params.handle;
		DMEMIT("%s %llu %u",
		       hc->devs.cache.dev->name,
		       (unsigned long long) hc->devs.cache.start,
		       hc->params.params);

		if (hc->params.params)
			DMEMIT(" %s", handle_str(handle));

		if (hc->params.params > 1)
			DMEMIT(" %llu",
			       (unsigned long long) hc->params.cache_size);

		if (hc->params.params > 2)
			DMEMIT(" read%s", OrigWritable(hc) ? "write" : "only");

		if (handle != OPEN_CACHE && hc->params.params > 3)
			DMEMIT(" %llu",
			       (unsigned long long) hc->params.extent_size);

		DMEMIT(" %s %llu",
		       hc->devs.orig.dev->name,
		       (unsigned long long) hc->devs.orig.start);
	}

	return 0;
}

/* Provide io hints. */
static void hstore_io_hints(struct dm_target *ti, struct queue_limits *limits)
{
	struct hstore_c *hc = ti->private;

	blk_limits_io_min(limits, hc->extents.size);
	blk_limits_io_opt(limits, hc->extents.size);
}

/* Hstore interface. */
static struct target_type hstore_target = {
	.name = "hstore",
	.version = {1, 0, 0},
	.module = THIS_MODULE,
	.ctr = hstore_ctr,
	.dtr = hstore_dtr,
	.flush = hstore_flush,
	.map = hstore_map,
	.end_io = hstore_end_io,
	.presuspend = hstore_presuspend,
	.resume = hstore_resume,
	.message = hstore_message,
	.status = hstore_status,
	.io_hints = hstore_io_hints,
};

int __init dm_hstore_init(void)
{
	int r;

	/* Create global multithreaded workqueue for all hstore devices. */
	_hstore_wq = create_workqueue(DAEMON);
	if (!_hstore_wq)
		DM_ERR_RET(-ENOMEM, "failed to create workqueue");

	r = dm_register_target(&hstore_target);
	if (r) {
		destroy_workqueue(_hstore_wq);
		DMERR("Failed to register %s [%d]", DM_MSG_PREFIX, r);
	}

	if (!r)
		DMINFO("registered %s %s", DM_MSG_PREFIX, version);

	return r;
}

void dm_hstore_exit(void)
{
	dm_unregister_target(&hstore_target);
	destroy_workqueue(_hstore_wq);
	DMINFO("unregistered %s %s", DM_MSG_PREFIX, version);
}

/* Module hooks */
module_init(dm_hstore_init);
module_exit(dm_hstore_exit);

MODULE_DESCRIPTION(DM_NAME "device-mapper hstore (hierarchical store) target");
MODULE_AUTHOR("Heinz Mauelshagen <heinzm@redhat.com>");
MODULE_LICENSE("GPL");
MODULE_ALIAS("dm-devcache");
