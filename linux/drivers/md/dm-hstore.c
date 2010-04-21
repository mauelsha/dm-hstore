/*
 * Copyright (C) 2009-2010 Red Hat GmbH. All rights reserved.
 *
 * Module Author: Heinz Mauelshagen <heinzm@redhat.com>
 *
 * This file is released under the GPL.
 *
 * Hierachical Storage (Caching) target to stack on top
 * of arbitrary other (readonly) block device (eg. iSCSI).
 *
 * Features:
 * o Caches reads and writes keeping persistent state metadata
 *   to allow for dirty cache restartss
 * o Writes back in order to enhance streaming performance
 *   on fragmented access pattern
 * o Can run on top of readonly original device (eg. on DVD-ROM)
 * o If so, writes back any dirty areas when set readwrite (useful for tests)
 * o readonly <-> readwrite access changes supported via message interface
 * o Initializes metadata for extents in cache in the background
 *   in order to fasten cache construction
 * o supports cache resizing via message interface or constructor
 * o stores CRCs with metadata and runs integrity checks on read
 * o stores versions with metadata to support future metadata migration
 o o barrier IO supported
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
 * H  : Cache header
 * Dn : cached data sectors belonging to n
 * Mn : metadata for n
 *
 * FIXME:
 * o make SECTOR sizes and offsets device agnostic
 *
 */

static const char version[] = "0.194";

#include "dm.h"

#include <linux/dm-io.h>
#include <linux/dm-kcopyd.h>
#include <linux/bio.h>
#include <linux/blkdev.h>
#include <linux/crc32.h>
#include <linux/init.h>
#include <linux/module.h>
#include <linux/slab.h>
#include <linux/vmalloc.h>
#include <linux/workqueue.h>

#define	TARGET		"dm-hstore"
#define	DAEMON		TARGET	"d"
#define	DM_MSG_PREFIX	TARGET

/* FIXME: factor these macros out to dm.h */
#define	TI_ERR_RET(str, ret) \
	do { ti->error = DM_MSG_PREFIX ": " str; return ret; } while (0);
#define	TI_ERR(str)	TI_ERR_RET(str, -EINVAL)
#define	DM_ERR_RET(ret, x...) \
	do { DMERR(x); return ret; } while (0);
#define	DM_ERR(x...)	DM_ERR_RET(-EINVAL, x)

#define	EXTENT_SECTORS_DEFAULT	to_sector(64*2048)	/* 64KB */
#define	SECTOR_SIZE		to_bytes(1)	/* Sector size in bytes. */

#define     range_ok(i, min, max)   (i >= min && i <= max)

/*
 * Fixed size of all ondisk metadata areas
 * (ie. header and per extent metadata).
 */
#define	META_SECTORS	to_sector(4096)
#define	EXTENTS_MIN	1
#define	MIN_CACHE_SIZE	(8*2*1024) /* 8MB */

/* Minimum parallel IO for resource allocation in dm_*() client creation. */
static unsigned parallel_io_min = 32;

/* Maximum parallel dirty extents flushing in order to minimize contention. */
static unsigned parallel_dirty_max = 3;

/* Maximum parallel extent creation in order to avoid starvation on writes. */
static unsigned parallel_init_max = 2048;

/* A hstore extent hash. */
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
	struct hstore_c *hc;	/* Needed for endio_put(extent). */

	/* Lists for various extent states. */
	struct {
		struct list_head hash;		/* Hash. */
		struct list_head dirty_flush;	/* Dirty and flush. */
		struct list_head endio;		/* Endio. */
		struct list_head free_init_lru;	/* Extent free/init/lru.*/
		atomic_t endio_ref;		/* # of endio references. */
	} lists;

	struct extent_io {
		struct bio_list in;	/* Input queue (extent good state). */
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

	struct {
		uint64_t dev;		/* Cache device size. */
		uint64_t extent;	/* Cache extent size. */
	} size;

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
	EXTENT_DIRTY,		/* Extent needs writing back to origin. */
	EXTENT_UPTODATE,	/* Extent data is uptodate. */
	EXTENT_ERROR,		/* IO error on extent ocurred. */
	EXTENT_FREE,		/* Extent is available. */
	/*
	 * Don't change the order of the previous ones
	 * because they are persistent in the ondisk metadata!
	 *
	 * Those following below are transient.
	 */
	EXTENT_INITIALIZING,	/* Extent initializing. */
	EXTENT_META_READ,	/* Extent metadata read. */
	EXTENT_COPYING,		/* Extent copy to/from origin in progress. */
	EXTENT_META_IO,		/* Extent metadata io active. */
};

BITOPS(Extent, Dirty, extent, EXTENT_DIRTY)
BITOPS(Extent, Uptodate, extent, EXTENT_UPTODATE)
BITOPS(Extent, Error, extent, EXTENT_ERROR)
BITOPS(Extent, Free, extent, EXTENT_FREE)

BITOPS(Extent, Initializing, extent, EXTENT_INITIALIZING)
BITOPS(Extent, MetaRead, extent, EXTENT_META_READ)
BITOPS(Extent, Copying, extent, EXTENT_COPYING)
BITOPS(Extent, MetaIo, extent, EXTENT_META_IO)

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

	struct {
		spinlock_t lock;	/* Protects central input list above. */
		struct bio_list in;	/* Pending bios (central input list). */
		struct bio_list work;	/* Bios work queue (no extents). */
		atomic_t ref;	/* IO in flight reference counting. */

		wait_queue_head_t suspendq;	/* Suspend synchronization. */

		struct workqueue_struct *wq; /* Work queue. */
		struct work_struct ws;	/* IO work. */

		/* IO services used. */
		struct dm_io_client *dm_io_client;
		struct dm_kcopyd_client *kcopyd_client;

		/* Mempool of metadata sectors. */
		mempool_t *metadata_pool;

		unsigned long flags;
	} io;

	/* Cache and original device properties. */
	struct devs {
		struct c_dev cache;
		struct c_dev orig;
	} devs;

	/* Extent properties. */
	struct {
		sector_t size;
		atomic_t free;		/* # of free extents. */
		atomic_t initialized;	/* # of initialized extents. */
		atomic_t lru;		/* # of least recently used extents. */
		atomic_t total;		/* Total # of extents. */
		atomic_t dirty_flushing;/* # of dirty flushing extents. */
		atomic_t init_max;	/* max # of init flushing extents. */

		/* Extent address masks to quicken calculation... */
		sector_t mask;		/* of hash key. */
		sector_t mask_inv;	/* of extent address. */
	} extents;

	struct hstore_lists {
		/* List of dirty extents. */
		struct list_head dirty;

		/* List of extents to end io in worker. */
		struct list_head endio;

		/* List of extents to io in do_flush(). */
		struct list_head flush;

		/* List of LRU ondisk cache extents. */
		struct list_head lru;

		/* List of free ondisk cache extents. */
		struct list_head free;

		/* List of extents to initialize in do_extents_init(). */
		struct list_head init;

		spinlock_t lock_endio; /* Protect endio list */
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
	CACHE_PERSISTENT,		/* Cache persistent. */
	CACHE_WRITE_THROUGH,		/* Write through cache. */
	/*
	 * Don't change the order of the previous ones
	 * because they are persistent in the ondisk metadata!
	 *
	 * Those following below are transient.
	 */

	CACHE_BARRIER,			/* Cache is performing barrier IO. */
	CACHE_CHANGE,			/* Request a cache property change. */
	CACHE_CHANGE_RW,		/* Toggle origin rw access. */
	CACHE_CHANGE_WRITE_POLICY,	/* Toggle cache write policy. */
	CACHE_INITIALIZED,		/* Cache completely initializated. */
	CACHE_INITIALIZE,		/* Cache initialization of extents. */
	CACHE_INITIALIZE_NEW,		/* Write " */
	CACHE_INITIALIZATION_ACTIVE,	/* Initialization IO active. */
	CACHE_RESIZE,			/* Cache resizing. */
	CACHE_SUSPEND,			/* Cache suspension. */
	CACHE_CACHE_IO_QUEUED,		/* IOs to cache device queued. */
	CACHE_ORIG_IO_QUEUED,		/* IOs to original device queued. */
	CACHE_NEW_BIOS_QUEUED,		/* New bios queued. */
	CACHE_STATISTICS,		/* Cache statisitics. */
};

BITOPS(Cache, Persistent, hstore_c, CACHE_PERSISTENT)
BITOPS(Cache, WriteThrough, hstore_c, CACHE_WRITE_THROUGH)

BITOPS(Cache, Barrier, hstore_c, CACHE_BARRIER)
BITOPS(Cache, Change, hstore_c, CACHE_CHANGE)
BITOPS(Cache, ChangeRW, hstore_c, CACHE_CHANGE_RW)
BITOPS(Cache, ChangeWritePolicy, hstore_c, CACHE_CHANGE_WRITE_POLICY)
BITOPS(Cache, Initialized, hstore_c, CACHE_INITIALIZED)
BITOPS(Cache, Initialize, hstore_c, CACHE_INITIALIZE)
BITOPS(Cache, InitializeNew, hstore_c, CACHE_INITIALIZE_NEW)
BITOPS(Cache, InitializationActive, hstore_c, CACHE_INITIALIZATION_ACTIVE)
BITOPS(Cache, Resize, hstore_c, CACHE_RESIZE)
BITOPS(Cache, Suspend, hstore_c, CACHE_SUSPEND)
BITOPS(Cache, CacheIOQueued, hstore_c, CACHE_CACHE_IO_QUEUED)
BITOPS(Cache, OrigIOQueued, hstore_c, CACHE_ORIG_IO_QUEUED)
BITOPS(Cache, NewBiosQueued, hstore_c, CACHE_NEW_BIOS_QUEUED)
BITOPS(Cache, Statistics, hstore_c, CACHE_STATISTICS)
#undef BITOPS

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

static void extent_init_address(struct hstore_c *hc, struct extent *extent,
				sector_t offset)
{
	extent->addr.cache.offset = hc->devs.cache.start + offset;
	extent->addr.orig.offset = 0;
	extent->io.flags = 0;
	SetExtentFree(extent);
}

/*
 * Disk metadata sectors alloc/free.
 */
static void *metadata_alloc(struct hstore_c *hc)
{
	return mempool_alloc(hc->io.metadata_pool, GFP_NOIO);
}

static void metadata_free(struct hstore_c *hc, void **disk_header)
{
	mempool_free(*disk_header, hc->io.metadata_pool);
	*disk_header = NULL;
}

/*
 * Extent struct allocation/free.
 */
static void *extent_alloc(struct hstore_c *hc, gfp_t flags)
{
	struct extent *extent = kzalloc(sizeof(*extent), flags);

	if (extent) {
		extent->hc = hc;
		INIT_LIST_HEAD(&extent->lists.hash);
		INIT_LIST_HEAD(&extent->lists.dirty_flush);
		INIT_LIST_HEAD(&extent->lists.endio);
		INIT_LIST_HEAD(&extent->lists.free_init_lru);
		atomic_set(&extent->lists.endio_ref, 0);
		bio_list_init(&extent->io.in);
		bio_list_init(&extent->io.endio);
		spin_lock_init(&extent->io.endio_lock);
	}

	return extent;
}

static void extent_free(struct extent *extent)
{
	kfree(extent);
}

/* Free a list of cache extent structures. */
static void extents_free_list(struct list_head *list)
{
	struct list_head *pos, *tmp;

	list_for_each_safe(pos, tmp, list) {
		list_del(pos);
		extent_free(list_entry(pos, struct extent, lists.free_init_lru));
	}
}

/* Free the INIT and LRU lists. */
static void cache_extents_free(struct hstore_c *hc)
{
	extents_free_list(&hc->lists.init);
	BUG_ON(!list_empty(&hc->lists.init));
	extents_free_list(&hc->lists.lru);
	BUG_ON(!list_empty(&hc->lists.lru));
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
	return atomic_read(&hc->io.ref);
}

/* Check cache idle. */
static int cc_idle(struct hstore_c *hc)
{
	int r = !cc_ios_inflight(hc) &&
		bio_list_empty(&hc->io.work);

	spin_lock_irq(&hc->io.lock);
	r &= bio_list_empty(&hc->io.in);
	spin_unlock_irq(&hc->io.lock);

	return r;
}

/* Derive hash key from bio sector. */
static inline sector_t _bio_to_key(struct hstore_c *hc, struct bio *bio)
{
	return bio->bi_sector & hc->extents.mask;
}

/* Derive offset within extent from bio. */
static inline sector_t _bio_to_extent(struct hstore_c *hc, struct bio *bio)
{
	return bio->bi_sector & hc->extents.mask_inv;
}

/*
 * Remap bio:
 *
 * o to origin in case of write through cache
 * -or-
 * o to sector offset relative to extent start on cache
 *   device in case of write back cache.
 */
static inline void bio_remap(struct extent *extent, struct bio *bio)
{
	struct hstore_c *hc = extent->hc;

	if (unlikely(CacheWriteThrough(hc) && bio_data_dir(bio) == WRITE))
		bio->bi_bdev = hc->devs.orig.dev->bdev;
	else {
		bio->bi_bdev = hc->devs.cache.dev->bdev;
		bio->bi_sector = extent->addr.cache.offset +
				 _bio_to_extent(hc, bio);
	}
}

/* Initialize a hash. */
static int hash_init(struct hstore_c *hc)
{
	unsigned buckets;
	sector_t extents = atomic_read(&hc->extents.total);
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
static inline struct list_head *hash_bucket(struct hash *hash, sector_t key)
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

/* Wake worker. */
static void do_hstore(struct work_struct *ws);
static void wake_do_hstore(struct hstore_c *hc)
{
	queue_work(hc->io.wq, &hc->io.ws);
}

/* Add an element to a list safely. */
static inline void _extent_add_safe(struct list_head *from,
				    struct list_head *to)
{
	if (list_empty(from))
		list_add_tail(from, to);
}

/* Delete an element from a list safely. */
static inline int _extent_del_safe(struct list_head *list)
{
	int r = !list_empty(list);

	if (r)
		list_del_init(list);

	return r;
}

/* return # of IO references on extent. */
static int endio_ref(struct extent *extent)
{
	return atomic_read(&extent->lists.endio_ref);
}

/* Get an IO reference for endio processing. */
static int endio_get(struct extent *extent)
{
	int r = 0;
	struct hstore_c *hc = extent->hc;

	atomic_inc(&hc->io.ref);

	if (!atomic_inc_return(&extent->lists.endio_ref)) {
		/* Take off endio list if on. */
		spin_lock_irq(&hc->lists.lock_endio);
		r = _extent_del_safe(&extent->lists.endio);
		spin_unlock_irq(&hc->lists.lock_endio);

	}

	return r;
}

/* Drop an endio reference and return true on zero. */
static int endio_drop(struct extent *extent)
{
	atomic_dec(&extent->hc->io.ref);
	return atomic_dec_and_test(&extent->lists.endio_ref);
}

/* Push an extent to the end of the endio list. */
static void endio_put(struct extent *extent)
{
	unsigned long flags;
	struct hstore_c *hc = extent->hc;

	/* Push to the endio list and flag to wake worker. */
	spin_lock_irqsave(&hc->lists.lock_endio, flags);
	_extent_add_safe(&extent->lists.endio, &hc->lists.endio);
	endio_drop(extent);
	spin_unlock_irqrestore(&hc->lists.lock_endio, flags);

	/* Wakeup worker to deal with endio list. */
	wake_do_hstore(hc);
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

/* Pop an extent off the endio list locked vs. endio routine. */
static struct extent *extent_endio_pop(struct hstore_c *hc)
{
	struct extent *extent;

	spin_lock_irq(&hc->lists.lock_endio);
	if (list_empty(&hc->lists.endio))
		extent = NULL;
	else {
		extent = list_first_entry(&hc->lists.endio,
					  struct extent, lists.endio);
		list_del_init(&extent->lists.endio);
	}

	spin_unlock_irq(&hc->lists.lock_endio);
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
					  struct extent, lists.dirty_flush);
		list_del_init(&extent->lists.dirty_flush);
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
					  struct extent, lists.free_init_lru);
		list_del_init(&extent->lists.free_init_lru);
	}

	return extent;
}

/* Common part of Endio function for extent_meta_io() and kcopy_endio(). */
static void endio(unsigned long error, struct extent *extent)
{
	if (unlikely(error))
		SetExtentError(extent);

	endio_put(extent);
}

/* Endio function for extent_meta_io(). */
static void meta_endio(unsigned long error, void *context)
{
	struct extent *extent = context;

	BUG_ON(!TestClearExtentMetaIo(extent));
	endio(error, extent);
}

/* Endio function for extent_data_io(); kcopyd used. */
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

	SetCacheCacheIOQueued(hc);
	return dm_io(&control, 1, &region, NULL);
}

/* Read/write a cache device extent struct */
static int extent_meta_io(int rw, struct extent *extent)
{
	struct hstore_c *hc = extent->hc;

	/* Removeme: statistics. */
	atomic_inc(hc->stats.extent_meta_io + !!(rw == WRITE));
	BUG_ON(TestSetExtentMetaIo(extent));
	return io(hc, extent->disk, 1,
		  extent->addr.cache.offset + hc->extents.size,
		  rw, meta_endio, extent);
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
	dh->size.dev = cpu_to_le64(dh->size.dev);
	dh->size.extent = cpu_to_le64(dh->size.extent);
	dh->flags = cpu_to_le64(dh->flags);
}

static void header_to_cpu(struct hstore_c *hc)
{
	struct disk_header *dh = hc->disk_header;

	dh->crc = le32_to_cpu(dh->crc);
	dh->size.dev = le64_to_cpu(dh->size.dev);
	dh->size.extent = le64_to_cpu(dh->size.extent);
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
	dh->size.dev = hc->devs.cache.size;
	dh->size.extent = hc->extents.size;
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
	    !dh->size.dev || !dh->size.extent ? -EINVAL : 0;

	return (r && !hm) ? -EPERM : r;
}

/* Lock and pop a bio safely off the endio list. */
static struct bio *_bio_list_pop_safe(struct extent *extent,
				      struct bio_list *endio_list)
{
	struct bio *bio;

	spin_lock_irq(&extent->io.endio_lock);
	bio = bio_list_pop(endio_list);
	spin_unlock_irq(&extent->io.endio_lock);

	return bio;
}

/*
 * bio_endio() an extents endio write bio_list
 * stopping in case metadata ain't written yet.
 */
static void extent_endio_bio_list(struct extent *extent)
{
	if (!ExtentMetaIo(extent)) {
		int error = ExtentError(extent) ? -EIO : 0;
		struct bio *bio;

		while ((bio = _bio_list_pop_safe(extent, &extent->io.endio)))
			bio_endio(bio, error);
	}
}

/* Add extent to end of init list. */
static void extent_init_add(struct extent *extent)
{
	_extent_add_safe(&extent->lists.free_init_lru, &extent->hc->lists.init);
}

/* Add extent to end of dirty list. */
static void extent_dirty_add(struct extent *extent)
{
	struct hstore_c *hc = extent->hc;

	_extent_del_safe(&extent->lists.dirty_flush);
	_extent_add_safe(&extent->lists.dirty_flush, &hc->lists.dirty);
}

/* Add extent to end of free/LRU list. */
static void extent_free_lru_add(struct extent *extent)
{
	if (list_empty(&extent->lists.free_init_lru)) {
		struct hstore_c *hc = extent->hc;

		if (ExtentFree(extent)) {
			list_add_tail(&extent->lists.free_init_lru,
				      &hc->lists.free);
			atomic_inc(&hc->extents.free);
		} else {
			list_add_tail(&extent->lists.free_init_lru,
				      &hc->lists.lru);
			atomic_inc(&hc->extents.lru);
		}
	}
}

/* Remove extent from LRU list. */
static void extent_free_lru_del(struct extent *extent)
{
	if (!list_empty(&extent->lists.free_init_lru)) {
		list_del_init(&extent->lists.free_init_lru);
		atomic_dec(ExtentFree(extent) ? &extent->hc->extents.free :
						&extent->hc->extents.lru);
	}
}

/* Return # of free extents. */
static unsigned extents_free(struct hstore_c *hc)
{
	return atomic_read(&hc->extents.free);
}

/*
 * Pop an extent off the free/LRU list triggering
 * any new metadata header writes in chunks.
 */
struct extent *extent_free_lru_pop(struct hstore_c *hc)
{
	unsigned free = extents_free(hc), lru = atomic_read(&hc->extents.lru);
	struct extent *extent;
	
	if (!(free + lru))
		return NULL;

	if (!CacheInitialized(hc) && free < 2) {
		/* None while active. */
		if (CacheInitializationActive(hc))
			return NULL;

		/*
		 * Can't fetch from LRU list in case of only 1 free
		 * extent if still initializing metadata.
		 *
		 * Need to write more free metadata extents
		 * first in order to grow the free list.
		 */
		if (CacheInitializeNew(hc)) {
			if (CachePersistent(hc))
				SetCacheInitialize(hc);

			return NULL;
		} 
	}

	/* Fetch one extent from proper list. */
	extent = list_first_entry(free ? &hc->lists.free : &hc->lists.lru,
				  struct extent, lists.free_init_lru);
	extent_free_lru_del(extent);
	return extent;
}

/* Add extent to end of flush list. */
static void extent_flush_add(struct extent *extent)
{
	_extent_del_safe(&extent->lists.dirty_flush);
	_extent_add_safe(&extent->lists.dirty_flush, &extent->hc->lists.flush);
}

/* Insert an entry into the extent hash. */
static void extent_hash_insert(struct extent *extent)
{
	_extent_del_safe(&extent->lists.hash);
	hash_insert(&extent->hc->hash, extent);	/* Hash the extent. */
}

/* Original device writable ? */
static inline int OrigWritable(struct hstore_c *hc)
{
	return hc->devs.orig.mode & FMODE_WRITE;
}

/* Try to get an extent either from the hash or off the LRU list. */
static struct extent *extent_get(struct hstore_c *hc, struct bio *bio)
{
	/* Key is relative (0 based) start address of extent on origin. */
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
		extent_free_lru_del(extent);
		goto out;
	}

	/*
	 * If it's not in the hash while we're
	 * still reading the cache metadata ->
	 * wait until all extents have been read.
	 */
	if (!CacheInitialized(hc) &&
	    !CacheInitializeNew(hc) &&
	    bio_data_dir(bio) == READ)
		goto out;

	/* Try to fetch an extent off the LRU list. */
	extent = extent_free_lru_pop(hc);
	if (extent) {
		BUG_ON(!bio_list_empty(&extent->io.in));
		BUG_ON(!bio_list_empty(&extent->io.endio));
		BUG_ON(ExtentMetaIo(extent));
		BUG_ON(ExtentCopying(extent));

		/* Reset state, adjust key and insert into hash. */
		extent->io.flags = 0;
		extent->addr.orig.key = hc->devs.orig.start + key;
		extent_hash_insert(extent);
	}

out:
	return extent;
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
		extent_init_address(hc, extent, offset);
		extent_init_add(extent);
	}

	return 0;
}

/* Resize cache device. */
static int cache_resize(struct hstore_c *hc, sector_t cache_size)
{
	int grow, r;
	unsigned count, free = 0;
	sector_t extents, extents_old, size_old;
	struct c_dev *cache = &hc->devs.cache;
	struct extent *extent;
	struct list_head list, *pos = &hc->lists.lru;

	if (cache_size < extents_start(hc))
		return 0;

	/* Check cache device limits. */
	if (cache_size > to_sector(i_size_read(cache->dev->bdev->bd_inode)))
		DM_ERR("device size %llu invalid",
		       (unsigned long long) cache_size);

	/* Calculate absolute number of extents fitting cache device size. */
	extents = cache_size - extents_start(hc);
	do_div(extents, extent_size(hc));

	if (extents < EXTENTS_MIN) {
		DMERR("cache size requested is too small");
		goto err;
	}

	extents_old = atomic_read(&hc->extents.total);
	if (extents == extents_old) {
		DMERR("cache size wouldn't change");
		goto err;
	}

	/* Grow: try allocating additional extent structures. */
	grow = extents > extents_old;
	if (grow) {
		sector_t start = extents_start(hc) +
				 extents_old * extent_size(hc),
			 tmp = extents - extents_old;

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
	list_for_each_entry(extent, &hc->lists.lru, lists.free_init_lru) {
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
		atomic_add(count, &hc->extents.lru);
	} else if (free == count) {
		unsigned i = count;

		/* Move extent to free below off the LRU list. */
		INIT_LIST_HEAD(&list);
		while (i--)
			list_move_tail(hc->lists.lru.next, &list);

		/* Adjust extent counters. */
		atomic_sub(count, &hc->extents.total);
		atomic_sub(count, &hc->extents.free);
		atomic_sub(count, &hc->extents.initialized);
		atomic_sub(count, &hc->extents.lru);
	} else {
		/* Enough free extents to shrink ? */
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
	metadata_free(hc, (void **) &hc->disk_header);
	if (r) {
		DMERR("FATAL: Error writing cache header to %s",
		      cache->dev->name);
		hc->devs.cache.size = size_old;
		goto err_free;
	}

	/*
	 * Tell the daemon to initialize any new extents
	 * or free any extents we shrunk the cache by.
	 */
	if (grow) {
		SetCacheInitializeNew(hc);
		ClearCacheInitialized(hc);
	} else
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

	/* REMOVEME: statistics. */
	atomic_inc(hc->stats.extent_data_io + !!(rw == WRITE));

	/* Check for partial extent at origin device end. */
	cache.count = orig.count =
	    min(hc->extents.size, hc->devs.orig.size - orig.sector);
	/* Set source and destination. */
	rw == READ ? (from = &orig,  to = &cache) :
		     (from = &cache, to = &orig);
	SetCacheCacheIOQueued(hc);
	SetCacheOrigIOQueued(hc);
	return dm_kcopyd_copy(hc->io.kcopyd_client, from, 1, to, 0,
			      kcopy_endio, extent);
}

/* Defer a bio to the hstore_c in case of no extent for later processing. */
static void queue_bio_to_hc(struct hstore_c *hc, struct bio *bio)
{
	bio_list_add(&hc->io.work, bio);
}

/*
 * Change hstore settings (rw access and cache
 * write policy) on message interface request.
 */
static void do_settings(struct hstore_c *hc)
{
	if (TestClearCacheChange(hc)) {
		int r = 0;
	
		if (TestClearCacheChangeRW(hc)) {
			if (OrigWritable(hc))
				hc->devs.orig.mode &= ~FMODE_WRITE;
			else
				hc->devs.orig.mode |= FMODE_WRITE;

			r++;
		}
	
		if (TestClearCacheChangeWritePolicy(hc)) {
			CacheWriteThrough(hc) ? ClearCacheWriteThrough(hc) :
						SetCacheWriteThrough(hc);
			r++;
		}
	
		/* If any state changed -> update cache header. */
		if (r && CachePersistent(hc))
			header_write(hc);
	}
}

/* IO whole extents (ie. copy data accross between cache and origin). */
static void extent_io(int rw, struct extent *extent)
{
	struct hstore_c *hc = extent->hc;

	/*
	 * Update state flags, allocate ondisk structure, transfer them
	 * to it and restore them (they will get updated in do_endios()).
	 */
	if (CachePersistent(hc)) {
		/* Save in core flags to restore. */
		unsigned long flags = extent->io.flags;

		/* Adjust flags for disk representation. */
		rw == READ ? SetExtentUptodate(extent) :
			     ClearExtentDirty(extent);
		extent->disk = metadata_alloc(hc);
		extent_to_disk(extent);

		/* Restore in core flags. */
		extent->io.flags = flags;

		/* Take out a references for the metadata IOs. */
		endio_get(extent);
	}

	BUG_ON(TestSetExtentCopying(extent));

	/* Take out a references for the extent data IOs. */
	endio_get(extent);
	BUG_ON(extent_data_io(hc, extent, rw));

	if (CachePersistent(hc))
		BUG_ON(extent_meta_io(WRITE, extent));
}

/* Process bios on an uptodate extent. */
static void bios_io(struct hstore_c *hc, struct extent *extent)
{
	int bios_queued = 0, on_endio_list = 0, writes = 0;
	struct bio *bio;

	BUG_ON(!ExtentUptodate(extent));

	/* Take io reference against premature endio while submitting bios. */
	on_endio_list = endio_get(extent);

	/*
	 * Remap all queued bios, take out an endio
	 * reference per bio and submit them.
	 */
	while ((bio = bio_list_pop(&extent->io.in))) {
		int write = !!(bio_data_dir(bio) == WRITE);

		/*
		 * Allow read during extent data IO but prohibit writes
		 * for cache/original device data consistency reasons.
		 */
		if (write && ExtentCopying(extent)) {
			bio_list_add_head(&extent->io.in, bio);
			break;
		}

		bio_remap(extent, bio);	/* Remap to cache or orig device. */
		endio_get(extent);	/* Take an io reference per bio. */
		generic_make_request(bio);

		bios_queued++;
		writes += write;

		/* REMOVEME: statistics. */
		atomic_inc(hc->stats.submitted_io + write);
	}

	/* If none queued -> clean up endio reference count and exit. */
	if (!bios_queued) {
		/* If the extent was on the endio list -> put it back on. */
		on_endio_list ? endio_put(extent) : endio_drop(extent);
		return;
	}

	SetCacheCacheIOQueued(hc);

	/*
	 * If I've got writes here *and* the extent hasn't been
	 * dirtied in a previous run -> update metadata on disk.
	 */
	if (writes) {
		int meta_io = 1;

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
		}

		/* Update metadata of this extent on cache device. */
		if (CachePersistent(hc) && meta_io && !ExtentMetaIo(extent)) {
			extent->disk = metadata_alloc(hc);
			BUG_ON(!extent->disk);
			extent_to_disk(extent);
			/* Take IO reference for metadata write. */
			endio_get(extent);
			BUG_ON(extent_meta_io(WRITE, extent));
		}
	}

	/* Releases reference against premature endio. */
	endio_put(extent);
}

/*
 * Assume all extents got validated ->
 * put them onto the free list.
 */
static void extents_init_to_free(struct hstore_c *hc, struct extent *extent)
{
	do {
		atomic_inc(&hc->extents.initialized);
		extent_free_lru_add(extent);
	} while ((extent = extent_init_pop(hc)));
}

/* Validate extents on cache initialization. */
static void extent_validate(struct extent *extent)
{
	struct hstore_c *hc = extent->hc;

	/* Unset active after max init extents and stop writing. */
	if (atomic_dec_and_test(&hc->extents.init_max)) {
		ClearCacheInitializationActive(hc);

		if (CacheInitializeNew(hc))
			ClearCacheInitialize(hc);
	}

	if (ExtentError(extent))
		DMERR_LIMIT("extent=%llu metadata read error",
	      	    	    (unsigned long long) extent->addr.cache.offset);

	/*
	 * Metadata read -> insert into hash if used, so
	 * that hash hits can start to happen in do_bios()->extent_get().
	 */
	if (TestClearExtentMetaRead(extent)) {
		if (ExtentFree(extent))
			/*
			 * A free extent got read and there'll be
			 * only free extents follwoing it ->
			 * we can move all other extents on
			 * the init list to the LRU list.
			 */
			extents_init_to_free(hc, extent);
		else {
			/* Used extent: insert into hash. */
			extent_hash_insert(extent);

			/*
			 * Put any left behind extents in
			 * need of IO on dirty/flush list.
			 */
			if (!ExtentUptodate(extent)) {
				if (TestClearExtentDirty(extent))
					DMERR_LIMIT("extent *not* uptodate "
						    "while dirty!");

				extent_flush_add(extent);
			} else if (ExtentDirty(extent)) {
				if (!TestSetExtentUptodate(extent))
					DMERR_LIMIT("extent dirty while "
						    "*not* uptodate!");

				extent_dirty_add(extent);
				/* REMOVEME: statistics */
				atomic_inc(&hc->stats.extent_dirty); 
			} else
				extent_free_lru_add(extent);

			atomic_inc(&hc->extents.initialized);
		}
	/* Metadata initial write of free extent. */
	} else {
		atomic_inc(&hc->extents.initialized);
		BUG_ON(!list_empty(&extent->lists.free_init_lru));
		extent_free_lru_add(extent);
	}

	/* All extents done. */
	if (atomic_read(&hc->extents.initialized) ==
	    atomic_read(&hc->extents.total)) {
		/* Flag done with extents initialization. */
		SetCacheInitialized(hc);
		ClearCacheInitializationActive(hc);
		ClearCacheInitialize(hc);
		ClearCacheInitializeNew(hc);
		atomic_set(&hc->extents.init_max, 0);
		DMINFO("initialized %s, %u total/%u free extents",
		       hc->devs.cache.dev->name,
		       atomic_read(&hc->extents.total), extents_free(hc));
	}
}

/* Handle all endios on extents. */
static void do_endios(struct hstore_c *hc)
{
	struct extent *extent;

	while ((extent = extent_endio_pop(hc))) {
		/* If present, a metadata IO has happened. */
		if (extent->disk &&
		    !ExtentMetaIo(extent)) {
			/* Transfer metadata to CPU on read. */
			if (ExtentMetaRead(extent)) {
				int r;
				sector_t offset = extent->addr.cache.offset;

				BUG_ON(!ExtentInitializing(extent));

				/*
				 * Need to set flags again, because they're
				 * transient and got overwritten from disk.
				 */
				extent_to_core(extent);

				/* Reset to enable access. */
				ClearExtentMetaIo(extent);
				ClearExtentCopying(extent);

				r = extent_check(extent, offset);
				SetExtentInitializing(extent);
				SetExtentMetaRead(extent);
				if (r) {
					/* Restore in case of error. */
					extent->addr.cache.offset = offset;
					SetExtentError(extent);
				}
			}

			/* Free disk header structure. */
			metadata_free(hc, (void **) &extent->disk);

			/*
			 * If flagged, this is extents metadata
			 * while initializing the cache, so we'll
			 * validate the metadata and carry on.
			 */
			if (TestClearExtentInitializing(extent)) {
				extent_validate(extent);
				continue;
			}
		}

		/*
		 * From here, we've got bio writes or extent copies.
		 */
/* xXx check if non-persistent cache endio still works! */

		/* End IO any writes *after* the metadata got updated. */
		extent_endio_bio_list(extent);

		/*
		 * Don't update state or put on lru/flush
		 * list until all io ends on extent.
		 */
		if (endio_ref(extent))
			continue;

		/* Cautious because of race between endio_put and do_endios. */
		spin_lock_irq(&hc->lists.lock_endio);
		_extent_del_safe(&extent->lists.endio);
		spin_unlock_irq(&hc->lists.lock_endio);

		/* Adjust extent state, if this was an extent copy. */
		if (TestClearExtentCopying(extent)) {
			if (!TestSetExtentUptodate(extent)) {
				/* Extent read off origin; may not be dirty! */
				BUG_ON(ExtentDirty(extent));
			} else if (TestClearExtentDirty(extent)) {
				/*
				 * Extent written to origin;
				 * needs to be uptodate.
				 */
				BUG_ON(!ExtentUptodate(extent));
				atomic_dec(&hc->extents.dirty_flushing);
				/* REMOVEME: statistics. */
				atomic_dec(&hc->stats.extent_dirty); 
			}
		}

		/*
		 * If there's no new bios to process ->
		 * put on dirty or LRU list.
		 */
		if (bio_list_empty(&extent->io.in)) {
			if (ExtentDirty(extent))
				extent_dirty_add(extent);
			else
				/* No bios and not dirty -> put on LRU list. */
				extent_free_lru_add(extent);
		} else
			/* There's bios pending -> put on flush list. */
			extent_flush_add(extent);
	}
}

/* Initialize any extents on init list or resize cache. */
/*
 * Initialize extent metadata by either reding them off the
 *  backing store in case of existing metadata or writing them to it
 * in case of a cache initialization or writing of an init extent.
 */
static void do_extents_init(struct hstore_c *hc)
{
	if (!CacheInitialize(hc) ||
	    CacheInitializationActive(hc) ||
	    CacheSuspend(hc))
		return;
	else {
		int rw = CacheInitializeNew(hc) ? WRITE : READ,
		    i = (rw == WRITE) ? parallel_init_max : 1;
		struct extent *extent;

		atomic_set(&hc->extents.init_max, i);

		do {
			extent = extent_init_pop(hc);
			if (!extent)
				break;

			extent->disk = metadata_alloc(hc);
			BUG_ON(!extent->disk);

			if (rw == WRITE) {
				ClearExtentMetaRead(extent);
				extent_to_disk(extent);
			} else
				SetExtentMetaRead(extent);

			SetExtentInitializing(extent);

			/* Flag it to be distinguishable in do_endios(). */
			SetCacheInitializationActive(hc);

			/* Take endio reference out and initiate IO. */
			endio_get(extent);
			BUG_ON(extent_meta_io(rw, extent));
		} while (--i);
	}
}

/* Resize cache on ctr argument request. */
static void do_resize(struct hstore_c *hc)
{
	if (!CacheInitialize(hc) && TestClearCacheResize(hc)) {
		int r = cache_resize(hc, hc->params.cache_new_size);

		if (r)
			SetCacheResize(hc);
	}
}

/* Restore caller context and safe extent reference for hstore_end_io(). */
static void _bio_restore_context(struct bio *bio, struct extent *extent)
{
	union map_info *map_context = bio->bi_private;

	bio->bi_private = map_context->ptr;
	map_context->ptr = extent;
}

/* Restore context and error bio. */
static void _bio_restore_and_error(struct bio *bio)
{
	/*
	 * Set map_context->ptr to NULL in order
	 * to avoid requeueing in hstore_end_io().
	 */
	_bio_restore_context(bio, NULL);
	bio_endio(bio, -EIO);
}

/*
 * Handle all incoming/deferred bios.
 *
 * The following extent states are handled here:
 *   o can't get extent from hash or LRU -> put bio off for later processing.
 *   o else merge bio into the bio queue of the extent and put extent
 *     onto flush list unless extent is active and not uptodate or
 *     it's a write (do_endios() will put those on flush list).
 */
static void do_bios(struct hstore_c *hc)
{
	int need_to_error;
	struct bio *bio;
	struct extent *extent;

	/* Any bios to work on? */
	if (!TestClearCacheNewBiosQueued(hc) &&
	    bio_list_empty(&hc->io.work))
		return;

	/* Quickly add any new bios queued to the end of the work list. */
	spin_lock_irq(&hc->io.lock);
	bio_list_merge(&hc->io.work, &hc->io.in);
	bio_list_init(&hc->io.in);
	spin_unlock_irq(&hc->io.lock);

	/* When handling a barrier -> wait untill all inflight ios finish. */
	if (CacheBarrier(hc) && cc_ios_inflight(hc))
		return;

	ClearCacheBarrier(hc);

	/*
	 * In case the original device isn't writable
	 * and the cache is full, I got to error the IO.
	 */
	need_to_error = !OrigWritable(hc) &&
			!extents_free(hc) &&
			!CacheInitialize(hc);

	/* Work all deferred or new bios on work list. */
	while ((bio = bio_list_pop(&hc->io.work))) {
		/* Once the barrier is flagged, defer further IO. */
		if (CacheBarrier(hc)) {
			queue_bio_to_hc(hc, bio);
			break;
		}

		/* Flag a barrier write. */
		if (bio_empty_barrier(bio))
			SetCacheBarrier(hc);

		/* If I can't get one -> put IO off (or error it; see below). */
		extent = extent_get(hc, bio);
		if (extent) {
			atomic_inc(&hc->stats.hits); /* REMOVEME: statistics */
	
			/*
			 * If extent is errored, error bio here,
			 * restoring bio private context before.
			 */
			if (ExtentError(extent)) {
				_bio_restore_and_error(bio);
				continue;
			}
	
			/* Restore bio private context. */
			_bio_restore_context(bio, extent);

			/*
			 * Put bio on extents input queue
			 * and extent on flush list,
			 */
			bio_list_add(&extent->io.in, bio);
			extent_flush_add(extent);
		} else {
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
			else {
				queue_bio_to_hc(hc, bio);
				break;
			}
		}
	}
}

/*
 * Check, if extents get completely written over and if so,
 * set them uptodate in order to suppress superfluous reads
 * from slow original device, hence raising performance.
 */
static void do_overwrite_check(struct hstore_c *hc)
{
	struct extent *extent;

	list_for_each_entry(extent, &hc->lists.flush, lists.dirty_flush) {
		/* Skip any uptodate or being copied extents. */
		if (ExtentUptodate(extent) ||
		    ExtentCopying(extent))
			continue;
		else {
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

/* Flush out any dirty extents in chunks. */
static void do_dirty(struct hstore_c *hc)
{
	/*
	 * Only put a maximum of any dirty
	 * extents on flush list in case:
	 *
	 *   o we're not asked to suspend
	 *   o the original device is writable
	 */
	if (!CacheSuspend(hc) &&
	    OrigWritable(hc)) {
		unsigned max = parallel_dirty_max;
		struct list_head *pos, *tmp;

		/* If busy, flush more aggressively. */
		if (!extents_free(hc)) {
			max *= 2;

			if (!atomic_read(&hc->extents.lru))
				max *= 50;
		}

		/* FIXME: all at once? */
		list_for_each_safe(pos, tmp, &hc->lists.dirty) {
			struct extent *extent = list_entry(pos, struct extent,
							   lists.dirty_flush);

			if (atomic_read(&hc->extents.dirty_flushing) >= max)
				break;

			BUG_ON(ExtentMetaIo(extent));
			BUG_ON(ExtentCopying(extent));
			extent_flush_add(extent);
			atomic_inc(&hc->extents.dirty_flushing);
		}
	}
}

/*
 * Walk the list of extents on flush list:
 *
 *   o not uptodate in cache -> read it from origin into cache
 *   o uptodate in cache -> read/write bios to/from it in cache;
 *     set it dirty in case of writes
 *   o dirty in cache and no bios -> write it out from cache to origin
 */
static void do_flush(struct hstore_c *hc)
{
	struct extent *extent;

	/* Work all extents on flush list. */
	while ((extent = extent_flush_pop(hc))) {
		if (ExtentUptodate(extent)) {
			/*
			 * Extent is uptodate and dirty; we can write
			 * it out to the origin or submit bios.
			 *
			 * Fatal if extent not dirty!
			 */
			if (bio_list_empty(&extent->io.in)) {
				if (!ExtentCopying(extent)) {
					BUG_ON(!ExtentDirty(extent));
					/* Write extent out to the origin. */
					extent_io(WRITE, extent);
				}
			} else 
				/*
				 * Submit any bios hanging off this extent.
				 * Avoid writes to writing extent in bios_io().
				 */
				bios_io(hc, extent);
		} else if (!ExtentCopying(extent)) {
			/*
			 * If the extent isn't uptodate ->
			 * read it in and update extent metadata.
			 *
			 * Fatal if extent dirty!
			 */
			BUG_ON(ExtentDirty(extent));
			extent_io(READ, extent);
		}
	}
}

/* Unplug: let any queued io role on the cache and/or origin devices. */
static void do_unplug(struct hstore_c *hc)
{
	if (TestClearCacheCacheIOQueued(hc))
		blk_unplug(bdev_get_queue(hc->devs.cache.dev->bdev));

	if (TestClearCacheOrigIOQueued(hc))
		blk_unplug(bdev_get_queue(hc->devs.orig.dev->bdev));

}

/* Wake up any waiters in case we're idle. */
static void do_wake(struct hstore_c *hc)
{
	/* Wake up any suspend waiter. */
	if (cc_idle(hc))
		wake_up(&hc->io.suspendq);
}

/*
 * Hierarchical storage worker thread.
 *
 * o do setting changes requested via message interface
 * o handle all outstanding endios on extents
 * o resize cache if requested by constructor/message interface
 * o work on all new queued bios putting them on extent bio queues
 * o initialize any uninitialized extents
 *   (read preexisting in or write free new ones)
 * o check for extents, which get completely written over and
 *   avoid extent reads if not uptodate
 * o add any dirty extents to flush list
 * o submit IO for extents on flush list
 * o unplug cache/original device queues
 * o wake any suspend waiters if idle
 */
static void do_hstore(struct work_struct *ws)
{
	struct hstore_c *hc = container_of(ws, struct hstore_c, io.ws);

	do_settings(hc);
	do_endios(hc);
	do_resize(hc);
	do_bios(hc);
	do_extents_init(hc);
	do_overwrite_check(hc);
	do_dirty(hc);
	do_flush(hc);
	do_unplug(hc);
	do_wake(hc);
}

/*
 * Create or read the cache device header.
 */
static int cache_header_init(struct hstore_c *hc, enum handle_type handle)
{
	int r;

	hc->disk_header = metadata_alloc(hc);
	BUG_ON(!hc->disk_header);

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
		SetCacheInitializeNew(hc);
	} else {
		if (r)
			goto err;

		DMINFO("read cache device %s header",
		       hc->devs.cache.dev->name);
		hc->extents.size = hc->disk_header->size.extent;
		hc->devs.cache.size = hc->disk_header->size.dev;
		hc->io.flags |= hc->disk_header->flags;

		/* Flag extent initialization reads. */
		ClearCacheInitializeNew(hc);
	}

	ClearCacheInitialized(hc);

	/* Set masks for fast bio -> extent mapping. */
	hc->extents.mask_inv = hc->extents.size - 1;
	hc->extents.mask = ~hc->extents.mask_inv;
err:
	metadata_free(hc, (void **) &hc->disk_header);
	return r;
}

/*
 * Destruct a cache mapping.
 */
static void hstore_dtr(struct dm_target *ti)
{
	struct hstore_c *hc = ti->private;

	if (!hc)
		return;

	if (hc->io.wq)
		destroy_workqueue(hc->io.wq);

	cache_extents_free(hc);

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

	r = dm_get_device(ti, argv[0], dev->mode, &dev->dev);
	if (r) {
		TI_ERR("Device lookup failed");
	} else {
		/* Check cache device limits. */
		if (dev->size >
		    to_sector(i_size_read(dev->dev->bdev->bd_inode)))
			TI_ERR("Device size");
	}

	return 0;
}

/* Check helper: device sizes make sense? */
static int size_check(struct hstore_c *hc)
{
	sector_t cache_size, orig_size;
	struct devs *d = &hc->devs;

	orig_size = d->orig.size - d->orig.start;
	cache_size = atomic_read(&hc->extents.total) * hc->extents.size;
	if (orig_size < cache_size)
		DM_ERR("origin device size smaller than total cache data size");

	if (!multiple(d->cache.start, META_SECTORS))
		DM_ERR("cache offset is not divisable by %llu",
		       (unsigned long long) META_SECTORS);

	if (atomic_read(&hc->extents.total) < EXTENTS_MIN)
		DM_ERR("cache too small for extent size");

	return 0;
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
static int str_listed(const char *str, ...)
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
	if (str_listed(arg, "ro", "readonly", NULL))
		return READ;
	else if (str_listed(arg, "default", "rw", "readwrite", NULL))
		return WRITE;
	else
		return -EINVAL;
}

enum write_type { WRITEBACK, WRITETHROUGH };
static enum write_type cache_write_policy(const char *arg)
{
	if (str_listed(arg, "default", "wb", "writeback", NULL))
		return WRITEBACK;
	else if (str_listed(arg, "wt", "writethrough", NULL))
		return WRITETHROUGH;
	else
		return -EINVAL;
}

enum store_type { TRANSIENT, PERSISTENT };
static enum store_type cache_store_policy(const char *arg)
{
	if (str_listed(arg, "default", "persistent", "permanent", NULL))
		return PERSISTENT;
	else if (str_listed(arg, "transient", "volatile", NULL))
		return TRANSIENT;
	else
		return -EINVAL;
}

/* Allocate and initialize a hstore_c context. */
static int
context_alloc(struct dm_target *ti, char **argv,
	      sector_t cache_start, sector_t cache_size, sector_t extent_size,
	      int hstore_params, int orig_access, enum handle_type handle,
	      enum write_type write_policy, enum store_type store_policy)
{
	int r;
	unsigned num_pages = parallel_io_min;
	struct hstore_c *hc;

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
	hc->extents.size = extent_size ? extent_size : EXTENT_SECTORS_DEFAULT;

	init_waitqueue_head(&hc->io.suspendq);	/* Suspend waiters. */
	atomic_set(&hc->io.ref, 0);
	atomic_set(&hc->extents.free, 0);
	atomic_set(&hc->extents.initialized, 0);
	atomic_set(&hc->extents.lru, 0);
	atomic_set(&hc->extents.total, 0);
	atomic_set(&hc->extents.dirty_flushing, 0);
	atomic_set(&hc->extents.init_max, 0);
	bio_list_init(&hc->io.in);
	bio_list_init(&hc->io.work);
	spin_lock_init(&hc->io.lock);
	spin_lock_init(&hc->lists.lock_endio);
	INIT_LIST_HEAD(&hc->lists.dirty);
	INIT_LIST_HEAD(&hc->lists.endio);
	INIT_LIST_HEAD(&hc->lists.flush);
	INIT_LIST_HEAD(&hc->lists.lru);
	INIT_LIST_HEAD(&hc->lists.free);
	INIT_LIST_HEAD(&hc->lists.init);
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

	/* Change cache flags if requested by ctr arguments. */
	store_policy == PERSISTENT ? SetCachePersistent(hc) :
				     ClearCachePersistent(hc);
	write_policy == WRITEBACK ? ClearCacheWriteThrough(hc) :
				    SetCacheWriteThrough(hc);

	hc->disk_header = metadata_alloc(hc);
	BUG_ON(!hc->disk_header);

	r = header_write(hc);
	metadata_free(hc, (void **) &hc->disk_header);
	if (r) {
		ti->error = "Error writing cache device header";
		goto err;
	}

	atomic_set(&hc->extents.total, extents_total(hc));

	/* Check, if device sizes are valid. */
	r = size_check(hc);
	if (r)
		goto err;

	/*
	 * Try reaquiring the cache device when size
	 * in header differs from ctr parameter.
	 */
	if (cache_size && cache_size != hc->devs.cache.size) {
		sector_t cur_size = hc->devs.cache.size;

		dm_put_device(ti, hc->devs.cache.dev);
		hc->devs.cache.size = cache_size;
		r = get_dev(ti, argv, &hc->devs.cache);
		hc->devs.cache.size = cur_size;
		if (r)
			goto err;

		/* Flag for worker thread, that cache needs resizing. */
		hc->params.cache_new_size = cache_size;
		SetCacheResize(hc);
	}

	/* Calculate amount of kcopyd pages needed to copy extents. */
	do_div(extent_size, to_sector(PAGE_SIZE));
	num_pages *= extent_size;

	/* I use kcopyd to copy extents between cache and origin device. */
	r = dm_kcopyd_client_create(num_pages, &hc->io.kcopyd_client);
	if (r)
		goto err;

	/* Initialize IO hash */
	r = hash_init(hc);
	if (r) {
		DMERR("couldn't create extent hash");
		goto err;
	}

	/* Allocate extent structs and put them on init list. */
	r = cache_extents_alloc(hc, hc->devs.cache.size,
				extents_start(hc),
				atomic_read(&hc->extents.total), GFP_KERNEL);
	if (r)
		goto err;

	/* Create singlethreaded workqueue for this hstore device. */
	INIT_WORK(&hc->io.ws, do_hstore);
	hc->io.wq = create_singlethread_workqueue(DAEMON);
	if (!hc->io.wq) {
		DMERR("couldn't create work queue");
		r  =-ENOMEM;
		goto err;
	}

	/* Flag extent initialization needs doing by worker thread. */
	ClearCacheInitializationActive(hc);
	if (CachePersistent(hc))
		SetCacheInitialize(hc);
	else {
		/* Else do it here. */
		ClearCacheInitialize(hc);
		extents_init_to_free(hc, extent_init_pop(hc));
		SetCacheInitialized(hc);
		atomic_set(&hc->extents.init_max, 0);
	}

	/* No larger bios than the extent size and no boundary crossing. */
	ti->split_io = hc->extents.size;

	/* REMOVEME: statistics. */
	stats_init(&hc->stats);
	SetCacheStatistics(hc);
err:
	return r;
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
 *	     [default/rw/readwrite/ro/readonly \
 *	     [default/wb/writeback/wt/writethrough \
 *           [default/persistent/transient]]]]]
 *
 * 'auto' causes open of a cache with a valid header or
 * creation of a new cache if there's no vaild one sized to
 * cache_dev_size.
 * If 'auto' is being used on a non-existing cache without cache_dev_size
 * or cache_dev_size = 0, the constructor fails.
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
 * 2 + 'auto': the cache device will either be opened and tried to resize
 * 	       or get initialized and sized to cache_dev_size.
 * 3: on create (either implicit or via 'auto'),
 *    this cache_extens_size will be used.
 * 4: the origin device will be opened read only/read write,
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
	unsigned long long tmp;
	sector_t cache_size = 0, cache_start,
		 extent_size = EXTENT_SECTORS_DEFAULT;
	/* Defaults: persitent, writeback cache and open. */
	enum store_type store_policy = PERSISTENT;
	enum write_type write_policy = WRITEBACK;
	enum handle_type handle = OPEN_CACHE;

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

		/* Get cache extent size. */
		if (hstore_params > 2) {
			if (sscanf(argv[5], "%llu", &tmp) != 1 ||
			    !is_power_of_2(tmp) ||
			    (tmp && (tmp < META_SECTORS)))
				TI_ERR("Invalid cache extent size argument")

			extent_size = tmp;
		}

		/* Get origin rw mode. */
		if (hstore_params > 3) {
			orig_access = orig_rw(argv[6]);
			if (orig_access < 0)
				TI_ERR("Invalid original rw parameter");
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

	/* Got all parameters -> allocate hstore context. */
	r = context_alloc(ti, argv, cache_start, cache_size, extent_size,
			  hstore_params, orig_access,
			  handle, write_policy, store_policy);
	if (r)
		hstore_dtr(ti);

	return r;
}

/* Queues bios to the cache and wakes up worker thread. */
static inline void queue_bio(struct hstore_c *hc, struct bio *bio)
{
	spin_lock_irq(&hc->io.lock);
	bio_list_add(&hc->io.in, bio);
	spin_unlock_irq(&hc->io.lock);

	/* REMOVEME: statistics */
	atomic_inc(hc->stats.io + !!(bio_data_dir(bio) == WRITE));

	/* Wakeup worker to deal with bio input list. */
	SetCacheNewBiosQueued(hc);
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
/*
 * In case metadata is still being written, postpone endio
 * processing of writes to the daemon in order to make sure,
 * that it's being done afterwards.
 *
 * In case of read bios only, where no metadata update
 * happens because the extent is already uptodate,
 * I can avoid this.
 */
static int hstore_end_io(struct dm_target *ti, struct bio *bio,
			 int error, union map_info *map_context)
{
	int r = 0;
	struct extent *extent = map_context->ptr;

	if (extent) {
		/* Avoid end io looping on any rescheduled writes forever. */
		map_context->ptr = NULL;

		/* We've got a bio IO error and flag that on the extent. */
		if (unlikely(error))
			SetExtentError(extent);

		if (bio_data_dir(bio) == WRITE &&
		    ExtentMetaIo(extent)) {
			unsigned long flags;

			/*
			 * Need a spinlock here, because endios can
			 * be processed in parallel with my worker.
			 */
			spin_lock_irqsave(&extent->io.endio_lock, flags);
			bio_list_add(&extent->io.endio, bio);
			spin_unlock_irqrestore(&extent->io.endio_lock, flags);

			r = DM_ENDIO_INCOMPLETE;	/* Another shot. */
		}

		/* Drop the reference. */
		endio_put(extent);
	}

	return r;
}

/* Flush method. */
static void hstore_flush(struct dm_target *ti)
{
	struct hstore_c *hc = ti->private;

	flush_workqueue(hc->io.wq);

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
	 * Wakeup worker to:
	 *
	 * o kick off IO processing in case of any
	 *   left behind extents at device suspension.
	 *
	 * o initialize backing store after ctr
	 */
	wake_do_hstore(hc);
}

/*
 * Message handler functions.
 */
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
	SetCacheChange(hc);

	/*
	 * Wakeup worker to change access and kick off any dirty
	 * extents processing in case we switch origin from read
	 * to write access.
	 */
	wake_do_hstore(hc);
	return 0;
}

/* Message handler to resize cache device. */
static int msg_resize(struct hstore_c *hc, char *arg)
{
	unsigned long long tmp;

	/* Wait for initialization or resizing to finish. */
	if (CacheInitialize(hc) ||
	    CacheResize(hc))
		return -EPERM;

	if (sscanf(arg, "%llu", &tmp) != 1 ||
	    tmp < extents_start(hc) + EXTENTS_MIN * extent_size(hc))
		return -EINVAL;

	hc->params.cache_new_size = tmp;

	/* Flag worker thread has to resize the cache. */
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
	SetCacheChange(hc);
	wake_do_hstore(hc);
	return 0;
}

/* Message handler to change cache statistics status output. */
static int msg_statistics(struct hstore_c *hc, char *arg)
{
	int o = str_listed(arg, "on", "off", NULL), r = 0;

	if (o == 1)
		SetCacheStatistics(hc);
	else if (o == 2)
		ClearCacheStatistics(hc);
	else
		r = -EINVAL;

	return r;
}
/* Message method. */
static int hstore_message(struct dm_target *ti, unsigned argc, char **argv)
{
	if (argc == 1) {
		if (str_listed(argv[0], "flush", NULL)) {
			struct hstore_c *hc = ti->private;

			if (!CacheSuspend(hc)) {
				hstore_flush(ti);
				return 0;
			} else
				DM_ERR_RET(-EPERM, "Cache suspended");
		}
	} else if (argc == 2) {
		if (str_listed(argv[0], "access", NULL))
			return msg_access(ti->private, argv[1]);
		else if (str_listed(argv[0], "resize", NULL))
			return msg_resize(ti->private, argv[1]);
		else if (str_listed(argv[0], "write_policy", NULL))
			return msg_write_policy(ti->private, argv[1]);
		else if (str_listed(argv[0], "statistics", NULL))
			return msg_statistics(ti->private, argv[1]);
	}

	DMWARN("Unrecognised cache message received.");
	return -EINVAL;
}

/* Status output method. */
static int hstore_status(struct dm_target *ti, status_type_t type,
			 char *result, unsigned maxlen)
{
	unsigned dirty, handle, hits;
	ssize_t sz = 0;
	struct hstore_c *hc = ti->private;
	struct stats *s;

	if (!hc)
		return 0;

	switch (type) {
	case STATUSTYPE_INFO:
		s = &hc->stats;
		dirty = atomic_read(&s->extent_dirty);
		hits = atomic_read(&s->hits);
		DMEMIT
		    ("v=%s %s %s %s %s %s %s",
		     version,
		     OrigWritable(hc) ? "readwrite" : "readonly",
		     CacheInitialize(hc) ? "INIT" : "ready",
		     CachePersistent(hc) ? "persistent" : "transient",
		     CacheWriteThrough(hc) ? "writethrough" : "writeback",
		     dirty ? "DIRTY" : "clean",
		     dirty > atomic_read(&hc->extents.total) * 9 / 10 ?
		     "GROW!" : "ok");

		if (CacheStatistics(hc))
			DMEMIT(" es=%llu flgs=%lu bkts=%u r=%u w=%u "
			     "rd=%u wd=%u rs=%u ws=%u ca=%u dr=%u dw=%u "
			     "mr=%u mw=%u ed=%u ec=%u h=%u m=%u ov=%u "
			     "ef=%u ei=%u el=%u et=%u edf=%u",
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
			     atomic_read(s->extent_meta_io + 1),
			     dirty,
			     atomic_read(&s->extent_clear_uptodate),
			     atomic_read(&s->hits), atomic_read(&s->misses),
			     atomic_read(&s->overwrite),
			     extents_free(hc),
			     atomic_read(&hc->extents.initialized),
			     atomic_read(&hc->extents.lru),
			     atomic_read(&hc->extents.total),
			     atomic_read(&hc->extents.dirty_flushing));

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

	blk_limits_io_min(limits, 0);
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
	int r = dm_register_target(&hstore_target);

	if (r)
		DMERR("Failed to register %s [%d]", DM_MSG_PREFIX, r);
	else
		DMINFO("registered %s %s", DM_MSG_PREFIX, version);

	return r;
}

void dm_hstore_exit(void)
{
	dm_unregister_target(&hstore_target);
	DMINFO("unregistered %s %s", DM_MSG_PREFIX, version);
}

/* Module hooks */
module_init(dm_hstore_init);
module_exit(dm_hstore_exit);

MODULE_DESCRIPTION(DM_NAME "device-mapper hstore (hierarchical store) target");
MODULE_AUTHOR("Heinz Mauelshagen <heinzm@redhat.com>");
MODULE_LICENSE("GPL");
MODULE_ALIAS("dm-devcache");
