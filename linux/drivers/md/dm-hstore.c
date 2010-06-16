/*
 * Copyright (C) 2010 Red Hat GmbH. All rights reserved.
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
 *   to allow for dirty cache restarts
 * o Writes back in order to enhance streaming performance
 *   on fragmented access pattern
 * o Caching unit is called extent sized 2^N (size >= PAGE_SIZE)
 * o Fully-associative cache
 * o Can run on top of readonly original device (eg. on DVD-ROM)
 * o If so, writes back any dirty areas when set readwrite (useful for tests)
 * o readonly <-> readwrite access changes supported via message interface
 * o Initializes metadata for extents in cache in the background
 *   in order to fasten cache construction. I.e. immediate cache access
 * o supports cache resizing via message interface or constructor
 * o stores CRCs with metadata and runs integrity checks on read
 * o stores versions with metadata to support future metadata changes
 * o barrier IO supported
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
 * H  : Cache header storing cache and extent size etc.
 * Dn : cached data sectors belonging to n
 * Mn : metadata for n holding cache and original device offsets
 *
 * FIXME:
 * o make SECTOR sizes and offsets device agnostic
 * o check all ctr flag combinations
 * o hstore_merge isn't useful unless direct io
 *
 */

static const char version[] = "0.285";

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

/*
 * Configuration parameters.
 */
/* Minimum parallel IO for resource allocation in dm_*() client creation. */
#define	PARALLEL_IO_MAX	1024

/* Minimum/maximum parallel dirty extent flushs. */
#define PARALLEL_DIRTY_MIN	2
#define	PARALLEL_DIRTY_MAX	(PARALLEL_IO_MAX / 2)

/* Ensure PARALLEL_DIRTY_* settings are sane. */
#if PARALLEL_DIRTY_MIN >= PARALLEL_DIRTY_MAX
	#error  PARALLEL_DIRTY_MIN >= PARALLEL_DIRTY_MAX
#endif

/* Ensure PARALLEL_*_MAX settings are sane */
#if PARALLEL_DIRTY_MAX >= PARALLEL_IO_MAX
	#error  PARALLEL_DIRTY_MAX >= PARALLEL_IO_MAX
#endif
/*
 * End configuration parameters.
 */


/* FIXME: factor these macros out to dm.h */
#define	TI_ERR_RET(ret, str) \
	do { ti->error = DM_MSG_PREFIX ": " str; return ret; } while (0);
#define	TI_ERR(str)	TI_ERR_RET(-EINVAL, str)
#define	DM_ERR_RET(ret, x...) \
	do { DMERR(x); return ret; } while (0);
#define	DM_ERR(x...)	DM_ERR_RET(-EINVAL, x)

#define	EXTENT_SECTORS_DEFAULT	to_sector(512*1024)	/* 512KB */
#define	SECTOR_SIZE		to_bytes(1)	/* Sector size in bytes. */

#define     range_ok(i, min, max)   (i >= min && i <= max)

/*
 * Fixed size of all ondisk metadata areas
 * (ie. header and per extent metadata).
 */
#define	META_SECTORS	to_sector(4096)
#define	EXTENTS_MIN	1
#define	MIN_CACHE_SIZE	(8*2*1024) /* 8MB */

/* Maximum parallel extent creation in order to avoid starvation on writes. */
#define	PARALLEL_INIT_MAX	2048

/* Reasonable extent size maximum. */
#define	MAX_EXTENT_SIZE		32768

/* A hstore extent hash. */
struct extent_hash {
	struct list_head *hash;
	unsigned buckets;
	unsigned mask;
	unsigned prime;
	unsigned shift;
};

/*
 * On disk metadata for cached extents.
 */
static const char extent_magic[] = "DmHsToRe";
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
 * (those on cache device but not on origin device).
 */
struct extent {
	struct hstore_c *hc;	/* Needed for extent_endio_add(extent). */

	/* Lists for various extent states. */
	struct {
		struct list_head hash;		/* Hash. */
		struct list_head dirty_flush;	/* Dirty and flush. */
		struct list_head endio;		/* Endio. */
		struct list_head free_init_lru;	/* Extent free/init/lru.*/
		struct list_head ordered;	/* Address ordered list. */
		atomic_t endio_ref;		/* # of endio references. */
	} lists;

	struct extent_io {
		struct bio_list in[2];	/* Bio read/write input queues. */
		struct bio_list endio;	/* Bios to endio. */
		/* Lock for races bwtween worker and  bio_submit_callback(). */
		spinlock_t endio_lock;
		unsigned long flags;	/* Status flag */
		/* Flash dirty extent after this delay. */
		unsigned long dirty_expire;
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
static const char header_magic[] = "dm_hstore_HM4711";
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

/* Macros to access object state flags. */
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
	EXTENT_FORCE_DIRTY,	/* Extent needs to be dirtied again. */
	EXTENT_COPYING,		/* Extent copy to/from origin in progress. */
	EXTENT_COPY_IO,		/* Extent data copy io active. */
	EXTENT_INIT,		/* Extent to init. */
	EXTENT_META_IO,		/* Extent metadata io active. */
	EXTENT_META_READ,	/* Extent metadata read. */
};

BITOPS(Extent, Dirty, extent, EXTENT_DIRTY)
BITOPS(Extent, Uptodate, extent, EXTENT_UPTODATE)
BITOPS(Extent, Error, extent, EXTENT_ERROR)
BITOPS(Extent, Free, extent, EXTENT_FREE)

BITOPS(Extent, Copying, extent, EXTENT_COPYING)
BITOPS(Extent, CopyIo, extent, EXTENT_COPY_IO)
BITOPS(Extent, ForceDirty, extent, EXTENT_FORCE_DIRTY)
BITOPS(Extent, Init, extent, EXTENT_INIT)
BITOPS(Extent, MetaRead, extent, EXTENT_META_READ)
BITOPS(Extent, MetaIo, extent, EXTENT_META_IO)

/* REMOVEME: */
/* Development statistics. */
struct stats {
	atomic_t io[2];
	atomic_t hits[2];
	atomic_t misses[2];
	atomic_t deferred_io[2];
	atomic_t submitted_io[2];
	atomic_t extent_copy_active;
	atomic_t extent_data_io[2];
	atomic_t extent_meta_io[2];
	atomic_t bios_endiod[2];
	atomic_t extent_valid;
	atomic_t extent_clear_uptodate;
	atomic_t overwrite;
	atomic_t endio_active;
	atomic_t bios_requeued;
	atomic_t writes_while_copying;
	atomic_t force_dirty;
	atomic_t dropped_on_flush;
	atomic_t merge_bvec_fn;
	atomic_t evict_from_flush;
	atomic_t dirty_flushing_total;
	atomic_t extents_hashed;
};

/* Reset statistics variables. */
static void stats_init(struct stats *s)
{
	int i = ARRAY_SIZE(s->io);

	while (i--) {
		atomic_set(s->io + i, 0);
		atomic_set(s->hits + i, 0);
		atomic_set(s->misses + i, 0);
		atomic_set(s->deferred_io + i, 0);
		atomic_set(s->submitted_io + i, 0);
		atomic_set(s->extent_data_io + i, 0);
		atomic_set(s->extent_meta_io + i, 0);
		atomic_set(s->bios_endiod + i, 0);
	}

	atomic_set(&s->extent_copy_active, 0);
	atomic_set(&s->extent_valid, 0);
	atomic_set(&s->extent_clear_uptodate, 0);
	atomic_set(&s->overwrite, 0);
	atomic_set(&s->endio_active, 0);
	atomic_set(&s->bios_requeued, 0);
	atomic_set(&s->writes_while_copying, 0);
	atomic_set(&s->force_dirty, 0);
	atomic_set(&s->dropped_on_flush, 0);
	atomic_set(&s->merge_bvec_fn, 0);
	atomic_set(&s->evict_from_flush, 0);
	atomic_set(&s->dirty_flushing_total, 0);
	atomic_set(&s->extents_hashed, 0);
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
};

/* Cache context. */
struct hstore_c {
	/* Extent hash. */
	struct extent_hash hash;

	struct {
		spinlock_t lock;	/* Protects central input list above. */
		struct bio_list in[2];	/* Pending bios (central input lists).*/
		struct bio_list work[2];/* Bios work queues (no extents). */
		atomic_t ref;	/* IO in flight reference counting. */
		atomic_t reads; /* Reads queued. */

		wait_queue_head_t suspendq;	/* Suspend synchronization. */

		struct workqueue_struct *wq;	/* Work queue. */
		struct delayed_work ws;		/* IO work. */

		/* IO services used. */
		struct dm_io_client *dm_io_client;
		struct dm_kcopyd_client *kcopyd_client;

		/* Mempool of metadata sectors. */
		mempool_t *metadata_pool;

		unsigned long flags;
		unsigned long dirty_expire;
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
		atomic_t dirty;		/* # of dirty extents. */
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

		/* Address ordered list of all extents. */
		struct list_head ordered;

		spinlock_t lock_endio; /* Protect endio list */
	} lists;

	/* ctr parameters for status output etc. */
	struct {
		unsigned params;
		enum handle_type handle;
		struct mutex resize_lock;
		sector_t cache_size;
		sector_t cache_new_size;
		sector_t cache_requested_size;
		sector_t extent_size;
		int access;
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
	CACHE_ORIG_WRITABLE,		/* Original device writable. */

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
	CACHE_DEAD,			/* Fatal cache io error. */
	CACHE_INITIALIZED,		/* Cache completely initializated. */
	CACHE_DO_INITIALIZE,		/* Run initialization of extents. */
	CACHE_INITIALIZE_NEW,		/* Write " */
	CACHE_INITIALIZATION_ACTIVE,	/* Initialization IO active. */
	CACHE_RESIZE,			/* Cache resizing. */
	CACHE_SUSPEND,			/* Cache suspension. */
	CACHE_CACHE_IO_QUEUED,		/* IOs to cache device queued. */
	CACHE_ORIG_IO_QUEUED,		/* IOs to original device queued. */
	CACHE_NEW_BIOS_QUEUED,		/* New bios queued. */
	CACHE_READ_FREE_EXTENT,		/* Read free extent during init. */
	CACHE_STATISTICS,		/* Cache statisitics. */
};

BITOPS(Cache, Persistent, hstore_c, CACHE_PERSISTENT)
BITOPS(Cache, WriteThrough, hstore_c, CACHE_WRITE_THROUGH)
BITOPS(Cache, OrigWritable, hstore_c, CACHE_ORIG_WRITABLE)

BITOPS(Cache, Barrier, hstore_c, CACHE_BARRIER)
BITOPS(Cache, Change, hstore_c, CACHE_CHANGE)
BITOPS(Cache, ChangeRW, hstore_c, CACHE_CHANGE_RW)
BITOPS(Cache, ChangeWritePolicy, hstore_c, CACHE_CHANGE_WRITE_POLICY)
BITOPS(Cache, Dead, hstore_c, CACHE_DEAD)
BITOPS(Cache, Initialized, hstore_c, CACHE_INITIALIZED)
BITOPS(Cache, DoInitialize, hstore_c, CACHE_DO_INITIALIZE)
BITOPS(Cache, InitializeNew, hstore_c, CACHE_INITIALIZE_NEW)
BITOPS(Cache, InitializationActive, hstore_c, CACHE_INITIALIZATION_ACTIVE)
BITOPS(Cache, Resize, hstore_c, CACHE_RESIZE)
BITOPS(Cache, Suspend, hstore_c, CACHE_SUSPEND)
BITOPS(Cache, CacheIOQueued, hstore_c, CACHE_CACHE_IO_QUEUED)
BITOPS(Cache, OrigIOQueued, hstore_c, CACHE_ORIG_IO_QUEUED)
BITOPS(Cache, NewBiosQueued, hstore_c, CACHE_NEW_BIOS_QUEUED)
BITOPS(Cache, ReadFreeExtent, hstore_c, CACHE_READ_FREE_EXTENT)
BITOPS(Cache, Statistics, hstore_c, CACHE_STATISTICS)
#undef BITOPS

/* Return extent size (extent sectors + metadata sectors). */
static inline sector_t extent_data_size(struct hstore_c *hc)
{
	return hc->extents.size;
}

/* Return extent size (extent sectors + metadata sectors). */
static inline sector_t extent_total_size(struct hstore_c *hc)
{
	return extent_data_size(hc) + META_SECTORS;
}

/* Return start of first extent. */
static inline sector_t extents_start(struct hstore_c *hc)
{
	return hc->devs.cache.start + META_SECTORS;
}

static void extent_init_address(struct extent *extent, sector_t offset)
{
	extent->addr.cache.offset = extent->hc->devs.cache.start + offset;
	extent->addr.orig.offset = 0;
	extent->io.flags = 0;
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
		INIT_LIST_HEAD(&extent->lists.ordered);
		atomic_set(&extent->lists.endio_ref, 0);
		bio_list_init(extent->io.in);
		bio_list_init(extent->io.in + 1);
		bio_list_init(&extent->io.endio);
		spin_lock_init(&extent->io.endio_lock);
	}

	return extent;
}

/* Return true if cache has read/write bios on input queues. */
static int cache_has_input_bios_queued(struct hstore_c *hc)
{
	return !(bio_list_empty(hc->io.in) &&
		 bio_list_empty(hc->io.in + 1));
}

/* Return true if cache has read/write bios on work  queues. */
static int cache_has_work_bios_queued(struct hstore_c *hc)
{
	return !(bio_list_empty(hc->io.work) &&
		 bio_list_empty(hc->io.work + 1));
}

/* Return true if extent has read/write bios on input queues. */
static int extent_has_bios_queued(struct extent *extent)
{
	return !(bio_list_empty(extent->io.in) &&
		 bio_list_empty(extent->io.in + 1));
}

/* Return true if extent is idle. */
static int endio_ref(struct extent *extent);
static int extent_is_idle(struct extent *extent)
{
	return !(endio_ref(extent) ||
		 extent_has_bios_queued(extent) ||
		 ExtentDirty(extent) ||
		 ExtentForceDirty(extent) ||
		 ExtentCopying(extent));
}

/* Check no ios inflight. */
static int cache_ios_inflight(struct hstore_c *hc)
{
	return atomic_read(&hc->io.ref);
}

/* Check for maximum ios inflight. */
static int cache_max_ios_inflight(struct hstore_c *hc)
{
	return cache_ios_inflight(hc) >= PARALLEL_IO_MAX;
}

/* Check cache idle. */
static int cache_idle(struct hstore_c *hc)
{
	int r = !cache_ios_inflight(hc) &&
		!cache_has_work_bios_queued(hc);

	spin_lock_irq(&hc->io.lock);
	r = r && !cache_has_input_bios_queued(hc);
	spin_unlock_irq(&hc->io.lock);

	return r;
}

/* Add an element to a list safely. */
static inline void _extent_add_safe(struct list_head *from,
				    struct list_head *to)
{
	if (list_empty(from))
		list_add_tail(from, to);
}

/* Delete an element from a list safely. */
static inline void _extent_del_safe(struct list_head *list)
{

	if (!list_empty(list))
		list_del_init(list);
}

/* Free one extent. */
static void extent_free(struct extent *extent)
{
	BUG_ON(!extent_is_idle(extent));
	kfree(extent);
}

/* Free a list of cache extent structures listed on the free/init/lru list. */
static void extents_free_list(struct list_head *list)
{
	struct list_head *pos, *tmp;
	struct extent *extent;

	list_for_each_safe(pos, tmp, list) {
		list_del(pos);
		extent = list_entry(pos, struct extent, lists.free_init_lru);
		_extent_del_safe(&extent->lists.ordered);
		extent_free(extent);
	}
}

/* Factor out to dm.c */
static int multiple(sector_t a, sector_t b)
{
	sector_t r = a;

	do_div(r, b);
	return a == r * b;
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

	if (unlikely(CacheWriteThrough(hc) && bio_data_dir(bio) == WRITE)) {
		bio->bi_bdev = hc->devs.orig.dev->bdev;
		SetCacheOrigIOQueued(hc);
	} else {
		bio->bi_bdev = hc->devs.cache.dev->bdev;
		bio->bi_sector = extent->addr.cache.offset +
				 _bio_to_extent(hc, bio);
		SetCacheCacheIOQueued(hc);
	}
}

/* Return # of dirty extents. */
static unsigned extents_dirty(struct hstore_c *hc)
{
	return atomic_read(&hc->extents.dirty);
}

/* Return # of free extents. */
static unsigned extents_free(struct hstore_c *hc)
{
	return atomic_read(&hc->extents.free);
}

/* Return # of initialized extents. */
static unsigned extents_initialized(struct hstore_c *hc)
{
	return atomic_read(&hc->extents.initialized);
}

/* Return # of LRU extents. */
static unsigned extents_lru(struct hstore_c *hc)
{
	return atomic_read(&hc->extents.lru);
}

/* Return # of total extents. */
static unsigned extents_total(struct hstore_c *hc)
{
	return atomic_read(&hc->extents.total);
}

/* Remove extent from hash. */
static void extent_hash_del(struct extent *extent)
{
	if (!list_empty(&extent->lists.hash)) {
		list_del_init(&extent->lists.hash);

		/* REMOVEME: stats. */
		atomic_dec(&extent->hc->stats.extents_hashed);
	}
}

/* Initialize a hash. */
static int hash_init(struct hstore_c *hc)
{
	unsigned buckets = roundup_pow_of_two(extents_total(hc) >> 5);
	static unsigned hash_primes[] = {
		/* Table of primes for hash_fn optimization. */
		1, 2, 3, 7, 13, 27, 53, 97, 193, 389, 769,
		1543, 3079, 6151, 12289, 24593, 49157, 98317,
	};
	struct extent_hash *hash = &hc->hash;

	/* Allocate stripe hash buckets. */
	hash->hash = vmalloc(buckets * sizeof(*hash->hash));
	if (!hash->hash)
		return -ENOMEM;

	hash->buckets = buckets;
	hash->mask = buckets - 1;
	hash->shift = ffs(buckets);
	if (hash->shift > ARRAY_SIZE(hash_primes) - 1)
		hash->shift = ARRAY_SIZE(hash_primes) - 1;

	BUG_ON(hash->shift < 2);
	hash->prime = hash_primes[hash->shift];

	/* Initialize buckets. */
	while (buckets--)
		INIT_LIST_HEAD(hash->hash + buckets);

	return 0;
}

static void hash_exit(struct extent_hash *hash)
{
	if (hash->hash)
		vfree(hash->hash);
}

/* Extent hash function. */
static inline unsigned hash_fn(struct extent_hash *hash, sector_t key)
{
	return ((key * hash->prime) >> hash->shift) & hash->mask;
}

/* Return bucket within hash. */
static struct list_head *hash_bucket(struct extent_hash *hash, sector_t key)
{
	return hash->hash + hash_fn(hash, key);
}

/* Insert an entry into a hash. */
static inline void hash_insert(struct extent_hash *hash, struct extent *extent)
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

/* Wake worker delayed/immediately. */
static void wake_do_hstore_delayed(struct hstore_c *hc, unsigned long delay)
{
	if (!delay)
		cancel_delayed_work(&hc->io.ws);

	queue_delayed_work(hc->io.wq, &hc->io.ws, delay);
}

static void wake_do_hstore(struct hstore_c *hc)
{
	wake_do_hstore_delayed(hc, 0);
}

/* Unplug cache: let any queued io role on the cache and/or origin devices. */
static void unplug_cache(struct hstore_c *hc)
{
	if (TestClearCacheCacheIOQueued(hc))
		blk_unplug(bdev_get_queue(hc->devs.cache.dev->bdev));

	if (TestClearCacheOrigIOQueued(hc))
		blk_unplug(bdev_get_queue(hc->devs.orig.dev->bdev));
}

/* return # of IO references on extent. */
static int endio_ref(struct extent *extent)
{
	return atomic_read(&extent->lists.endio_ref);
}

/* Get an IO reference for endio processing. */
static void endio_get(struct extent *extent)
{
	if (atomic_inc_return(&extent->hc->io.ref) >= PARALLEL_IO_MAX)
		unplug_cache(extent->hc);

	atomic_inc(&extent->lists.endio_ref);
}

/* Drop an endio reference and return true on zero. */
static void endio_put(struct extent *extent)
{
	atomic_dec(&extent->lists.endio_ref);
	atomic_dec(&extent->hc->io.ref);
}

/* Push an extent to the end of the endio list. */
static void extent_endio_add(struct extent *extent)
{
	unsigned long flags;
	struct hstore_c *hc = extent->hc;

	/* Push to the endio list and flag to wake worker. */
	spin_lock_irqsave(&hc->lists.lock_endio, flags);
	_extent_add_safe(&extent->lists.endio, &hc->lists.endio);
	spin_unlock_irqrestore(&hc->lists.lock_endio, flags);
}

/* Transfer extent representation to disk/core with sizes on disk in bytes. */
static void extent_to_disk(struct extent *extent)
{
	struct extent_disk *ed = extent->disk;

	strncpy(ed->magic, extent_magic, sizeof(ed->magic));
	ed->flags = cpu_to_le64(extent->io.flags);
	ed->crc = ed->filler = 0;
	ed->addr.cache_offset =
		 cpu_to_le64(to_bytes(extent->addr.cache.offset));
	ed->addr.orig_offset = cpu_to_le64(to_bytes(extent->addr.orig.offset));
	ed->crc = cpu_to_le32(crc32(~0, ed, sizeof(*ed)));
}

static void extent_to_core(struct extent *extent)
{
	struct extent_disk *ed = extent->disk;

	extent->io.flags = le64_to_cpu(ed->flags);
	extent->addr.cache.offset =
		to_sector(le64_to_cpu(ed->addr.cache_offset));
	extent->addr.orig.offset = to_sector(le64_to_cpu(ed->addr.orig_offset));
	ed->crc = le32_to_cpu(ed->crc);
}

/* Check extent magic. */
static int extent_check(struct extent *extent, sector_t offset)
{
	struct hstore_c *hc = extent->hc;
	struct extent_disk *ed = extent->disk;
	unsigned crc = ed->crc;

	ed->crc = 0;
	if (strncmp(ed->magic, extent_magic, sizeof(ed->magic)) ||
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

/*
 * Common part of endio function for
 * extent_meta_io_async() and kcopy_endio().
 */
static void endio(unsigned long error, struct extent *extent)
{
	if (unlikely(error))
		SetExtentError(extent);

	extent_endio_add(extent);

	/* Wakeup worker to deal with endio list. */
	wake_do_hstore(extent->hc);
}

/* Endio function for extent_meta_io_async(). */
static void meta_endio(unsigned long error, void *context)
{
	struct extent *extent = context;

	BUG_ON(!TestClearExtentMetaIo(extent));

	if (unlikely(error && ExtentDirty(extent)))
		SetCacheDead(extent->hc);

	endio(error, extent);
}

/* Endio function for extent_data_io_async(); kcopyd used. */
static void kcopy_endio(int read_err, unsigned long write_err, void *context)
{
	struct extent *extent = context;

	BUG_ON(!TestClearExtentCopyIo(extent));
	endio((unsigned long) (read_err || write_err), extent);
}

/* Asynchronuous IO if fn != NULL, else synchronuous. */
static int io(int rw, struct c_dev *dev, struct hstore_c *hc, void *ptr,
	      sector_t sector, io_notify_fn fn, void *context)
{
	struct dm_io_region region = {
		.bdev = dev->dev->bdev,
		.sector = sector,
		.count = 1,
	};
	struct dm_io_request control = {
		.bi_rw = rw,
		.mem = { .type = DM_IO_KMEM, .ptr.addr = ptr },
		.notify = { .fn = fn, .context = context },
		.client = hc->io.dm_io_client,
	};

	SetCacheCacheIOQueued(hc);
	return dm_io(&control, 1, &region, NULL);
}

/* (A)synchronously read/write a cache device extent metadata struct. */
static int _extent_meta_io(int rw, struct extent *extent,
			   void (*endio_fn)(unsigned long error, void *context))
{
	struct hstore_c *hc = extent->hc;

	/* Removeme: statistics. */
	atomic_inc(hc->stats.extent_meta_io + !!(rw == WRITE));
	BUG_ON(TestSetExtentMetaIo(extent));

	/* Write metadata immediately after extent data w/o gap. */
	return io(rw, &hc->devs.cache, hc, extent->disk,
		  extent->addr.cache.offset + extent_data_size(hc),
		  endio_fn, extent);
}

/* Asynchronously read/write a cache device extent metadata struct. */
static int extent_meta_io_async(int rw, struct extent *extent)
{
	return _extent_meta_io(rw, extent, meta_endio);
}

/* Synchronously write a free extent header. */
static int extent_write_free_header_sync(struct extent *extent)
{
	int r;
	struct hstore_c *hc = extent->hc;

	extent->disk = metadata_alloc(hc);
	BUG_ON(!extent->disk);
	extent->io.flags = 0;
	SetExtentFree(extent);
	extent_to_disk(extent);
	r = _extent_meta_io(WRITE, extent, NULL);
	if (r)
		SetCacheDead(hc);

	ClearExtentMetaIo(extent);
	metadata_free(hc, (void **) &extent->disk);
	return r;
}

/* Read/write the cache device header synchronuously. */
static int header_io_sync(struct hstore_c *hc, int rw)
{
	return io(rw, &hc->devs.cache, hc, hc->disk_header,
		  hc->devs.cache.start, NULL, NULL);
}

/* Transfer cache device header from/to CPU with sizes on disk in bytes. */
static void header_to_disk(struct hstore_c *hc)
{
	struct disk_header *dh = hc->disk_header;

	dh->crc = 0;
	dh->crc = cpu_to_le32(crc32(~0, dh, sizeof(*dh)));
	dh->size.dev = cpu_to_le64(to_bytes(dh->size.dev));
	dh->size.extent = cpu_to_le64(to_bytes(dh->size.extent));
	dh->flags = cpu_to_le64(dh->flags);
}

static void header_to_core(struct hstore_c *hc)
{
	struct disk_header *dh = hc->disk_header;

	dh->crc = le32_to_cpu(dh->crc);
	dh->size.dev = to_sector(le64_to_cpu(dh->size.dev));
	dh->size.extent = to_sector(le64_to_cpu(dh->size.extent));
	dh->flags = cpu_to_le64(dh->flags);
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

	strncpy(dh->magic, header_magic, sizeof(dh->magic));
	header_version_init(dh);
	dh->size.dev = hc->devs.cache.size;
	dh->size.extent = extent_data_size(hc);
	/* Mask out any transient flags. */
	dh->flags = hc->io.flags & (CACHE_PERSISTENT |
				    CACHE_WRITE_THROUGH |
				    CACHE_ORIG_WRITABLE);
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
	r = header_io_sync(hc, WRITE);
	header_to_core(hc);

	if (r)
		SetCacheDead(hc);

	return r;
}

/* Check cache device header validity. */
static int header_check(struct hstore_c *hc)
{
	int r, hm;
	struct disk_header *dh = hc->disk_header;
	unsigned crc = dh->crc;

	dh->crc = 0;
	hm = strncmp(dh->magic, header_magic, sizeof(dh->magic));
	r = hm || !header_version_check(dh) ||
	    crc != crc32(~0, dh, sizeof(*dh)) ||
	    !dh->size.dev || !dh->size.extent ? -EINVAL : 0;

	return (r && !hm) ? -EPERM : r;
}

/* Pop a bio safely off the endio list. */
static struct bio *_bio_list_pop_safe(struct extent *extent,
				      struct bio_list *endio_list)
{
	struct bio *bio;

	spin_lock_irq(&extent->io.endio_lock);
	bio = bio_list_pop(endio_list);
	spin_unlock_irq(&extent->io.endio_lock);

	return bio;
}

/* bio_endio() an extents endio write bio_list. */
static void extent_endio_bio_list(struct extent *extent)
{
	int error = ExtentError(extent) ? -EIO : 0;
	struct bio *bio;

	while ((bio = _bio_list_pop_safe(extent, &extent->io.endio))) {
		BUG_ON(ExtentMetaIo(extent));
		BUG_ON(bio_data_dir(bio) == READ);

		bio_endio(bio, error);
		endio_put(extent);

		/* REMOVEME: stats. */
		atomic_inc(extent->hc->stats.bios_endiod + 1);
	}
}

/* Remove extent from flush list. */
static void extent_flush_del(struct extent *extent)
{
	_extent_del_safe(&extent->lists.dirty_flush);
}

/* Add to dirty list sorted by expire timestamp. */
static void extent_dirty_add_sorted(struct extent *extent)
{
	struct extent *e;
	struct list_head *insert = &extent->hc->lists.dirty;

	extent_flush_del(extent);

	list_for_each_entry(e, &extent->hc->lists.dirty, lists.dirty_flush) {
		if (extent->io.dirty_expire < e->io.dirty_expire)
			insert = &e->lists.dirty_flush;
		else
			break;
	}

	list_add(&extent->lists.dirty_flush, insert);
}

/* Add to free list sorted by ascending disk address. */
static void extent_free_add_sorted(struct extent *extent)
{
	struct extent *e;
	struct list_head *insert = &extent->hc->lists.free;

	list_for_each_entry(e, &extent->hc->lists.free, lists.free_init_lru) {
		if (extent->addr.cache.offset < e->addr.cache.offset)
			insert = &e->lists.free_init_lru;
		else
			break;
	}

	list_add(&extent->lists.free_init_lru, insert);
}

/* Add extent to end of free/init/LRU list. */
static void extent_free_init_lru_add(struct extent *extent)
{
	if (list_empty(&extent->lists.free_init_lru)) {
		struct hstore_c *hc = extent->hc;
		struct list_head *list;
		atomic_t *inc, dummy;

		BUG_ON(!extent_is_idle(extent));

		if (ExtentInit(extent))
			list = &hc->lists.init, inc = &dummy;
		else if (ExtentFree(extent))
			list = &hc->lists.free, inc = &hc->extents.free;
		else
			list = &hc->lists.lru, inc = &hc->extents.lru;

		/* Special case to add sorted to free list. */
		if (ExtentFree(extent))
			extent_free_add_sorted(extent);
		else
			list_add_tail(&extent->lists.free_init_lru, list);

		atomic_inc(inc);
	}
}

/* Remove extent from free/init/LRU list. */
static void extent_free_init_lru_del(struct extent *extent)
{
	if (!list_empty(&extent->lists.free_init_lru)) {
		list_del_init(&extent->lists.free_init_lru);

		if (!ExtentInit(extent)) {
			atomic_dec(ExtentFree(extent) ?
				   &extent->hc->extents.free :
				   &extent->hc->extents.lru);
		}
	}
}

/*
 * Pop an extent off the free/LRU list triggering
 * any new metadata header writes in chunks.
 */
struct extent *extent_free_lru_pop(struct hstore_c *hc)
{
	unsigned free = extents_free(hc), lru = extents_lru(hc);
	struct extent *extent;
	
	if (!(free + lru))
		return NULL;

	/* None while active. */
	if (CacheInitializationActive(hc))
		return NULL;

	if (!CacheInitialized(hc) && free < 2) {
		/*
		 * Can't fetch from LRU list in case of only 1 free
		 * extent if still initializing metadata.
		 *
		 * Need to write more free metadata extents
		 * first in order to grow the free list.
		 */
		if (CachePersistent(hc) && CacheInitializeNew(hc))
			SetCacheDoInitialize(hc);

		return NULL;
	}

	/* Fetch one extent from proper list. */
	extent = list_first_entry(free ? &hc->lists.free : &hc->lists.lru,
				  struct extent, lists.free_init_lru);
	extent_free_init_lru_del(extent);
	BUG_ON(!extent_is_idle(extent));
	return extent;
}

/* Add extent to end of flush list. */
static void extent_flush_add(struct extent *extent)
{
	_extent_del_safe(&extent->lists.dirty_flush);
	_extent_add_safe(&extent->lists.dirty_flush, &extent->hc->lists.flush);
}

/*
 * Sort flush list by extents with reads,
 * writes, non-uptodate and dirty extents.
 */
enum sort_type { BIO_READ = 0, BIO_WRITE, NOTUPTODATE, DIRTY, SORT_END };
static void sort_flush_list(struct hstore_c *hc)
{
	enum sort_type type;
	struct extent *extent;
	struct list_head sort[SORT_END];

	for (type = 0; type < SORT_END; type++)
		INIT_LIST_HEAD(sort + type);

	/* Put all extents on distinct lists. */
	while ((extent = extent_flush_pop(hc))) {
		if (!ExtentUptodate(extent))
			type = NOTUPTODATE;
		else if (!bio_list_empty(extent->io.in))
			type = BIO_READ;
		else if (!bio_list_empty(extent->io.in + 1))
			type = BIO_WRITE;
		else
			type = DIRTY;

		list_add_tail(&extent->lists.dirty_flush, sort + type);
	}

	/* Merge distinct lists in order. */
	for (type = 0; type < SORT_END; type++)
		list_splice_tail(sort + type, &hc->lists.flush);
}

/* Insert an entry into the extent hash, deleting it before if in. */
static void extent_hash_insert(struct extent *extent)
{
	/* Delete before reinserting with new key. */
	extent_hash_del(extent);
	hash_insert(&extent->hc->hash, extent);	/* Hash the extent. */

	/* REMOVEME: stats. */
	atomic_inc(&extent->hc->stats.extents_hashed);
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
		extent_free_init_lru_del(extent);
		goto out;
	}

	/*
	 * If it's not in the hash while we're
	 * still reading the cache metadata ->
	 * wait until all extents have been read.
	 */
	if (!CacheInitialized(hc) && !CacheInitializeNew(hc))
		goto out;

	/* Try to fetch an extent off the LRU list. */
	extent = extent_free_lru_pop(hc);
	if (extent) {
		/* Reset state, adjust key and insert into hash. */
		extent->io.flags = 0;
		extent->io.dirty_expire = 0;
		extent->addr.orig.key = hc->devs.orig.start + key;
		extent_hash_insert(extent);
	}

out:
	return extent;
}

/* Initialize cache extent structures in memory and add them to init list. */
static int cache_extents_alloc(struct hstore_c *hc, sector_t cache_size,
			       sector_t start, unsigned count,
			       struct list_head *list, gfp_t flags)
{
	sector_t increment = extent_total_size(hc), offset,
		 end = start + count * increment;
	struct extent *extent;

	for (offset = start; offset < end; offset += increment) {
		extent = extent_alloc(hc, flags);
		if (!extent)
			return -ENOMEM;

		/* All extents go to init list for later processing. */
		extent_init_address(extent, offset);
		SetExtentInit(extent);

		/* Add extent to end of list. */
		_extent_add_safe(&extent->lists.ordered, list);
	}

	return 0;
}

/* Read @sector off @dev and return @sector if readable, else 0. */
static sector_t check_dev_access(struct hstore_c *hc,
				 struct c_dev *dev, sector_t sector)
{
	int r;
	void *ptr = metadata_alloc(hc);

	BUG_ON(!ptr);
	r = io(READ, dev, hc, ptr, sector - 1, NULL, NULL);
	metadata_free(hc, &ptr);
	return r ? 0 : sector;
}

/*
 * Resize cache device.
 *
 * Checks address orderered list of extents backwards
 * on free or just adds to init list on grow.
 *
 * Done in 3 steps outlined below.
 */
static int cache_resize(struct hstore_c *hc)
{
	int grow, r;
	unsigned count, todo;
	sector_t cache_size = hc->params.cache_new_size,
		 cache_size_old, extents, extents_old;
	struct c_dev *cache = &hc->devs.cache;
	struct extent *extent, *tmp;
	struct list_head list;

	INIT_LIST_HEAD(&list);

	if (cache_size < extents_start(hc))
		return 0;

	/* Check cache device limits. */
	if (cache_size != check_dev_access(hc, cache, cache_size))
		DM_ERR("cache device size %llu too small",
		      (unsigned long long) cache_size);

	/* Calculate absolute number of extents fitting cache device size. */
	extents = cache_size - extents_start(hc);
	do_div(extents, extent_total_size(hc));

	if (extents < EXTENTS_MIN)
		DM_ERR("cache size requested is smaller than minimum %llu",
		      (unsigned long long) extents_start(hc) +
					   EXTENTS_MIN * extent_total_size(hc));

	extents_old = extents_total(hc);
	if (extents == extents_old)
		DM_ERR("cache size wouldn't change");

	/* Save given cache size for potential restore. */
	cache_size_old = cache->size;

	/*
 	 * Step 1: Either allocate new extents on grow or
 	 *	   pull any idle extents out of the work lists
 	 *	   (init/free/LRU lists that is)
	 */
	grow = extents > extents_old;
	if (grow) {
		/*
		 * Grow: try allocating additional extent structures;
	 	 *	 initially add to a private list so that we can bail
		 *	 out smoothly in case the header update should fail.
		 */
		count = todo = extents - extents_old;
		r = cache_extents_alloc(hc, cache_size, 
					extents_start(hc) +
					extents_old * extent_total_size(hc),
					count, &list, GFP_NOIO);
		if (r) {
			DMERR("can't allocate requested %u extents for %s",
			      count, cache->dev->name);
			goto err_free;
		}

		todo = 0;
		cache->size += count * extent_total_size(hc);
	} else {
		/*
		 * Shrink: move extents to be freed off the free/init/LRU lists.
		 *	   Again to a private list first to be
		 *	   able to stand a header write failure.
		 */
		/* One more to write the mandatory free one. */
		count = todo = extents_old - extents + 1;
		while (todo) {
			/*
			 * Check ordered list from the
			 * end for idle extents to free.
			 */
			extent = list_entry(hc->lists.ordered.prev,
					    struct extent, lists.ordered);
			if (extent_is_idle(extent)) {
				BUG_ON(!list_empty(&extent->lists.dirty_flush));
				BUG_ON(!list_empty(&extent->lists.endio));
				BUG_ON(endio_ref(extent));

				/* Move from ordered to private list. */
				list_move(&extent->lists.ordered, &list);
				SetExtentFree(extent);
				todo--;
			} else
				break;
		}

		/* None to free. */
		if (todo == count)
			goto out;

		if (count - todo)
			DMWARN_LIMIT("Freeing %u extents of requested %u.",
				     count - todo, count);

		cache->size -= (count - todo) * extent_total_size(hc);
	}

	/*
 	 * Step 2: Write header unless transient or no extents freed.
 	 *
 	 *	   If this is growing the cache, write next free
 	 *	   extent for the cache initialization to be
 	 *	   safe, because it expects a last free header..
	 */

	/* Update disk header with new cache size. */
	if (CachePersistent(hc)) {
		/*
		 * We have to write *one* free extent for the cache
		 * initialization to succeed in case we fail _after_
		 * writing the header but before the worker starts
		 * initializing the new extents on disk.
		 */
		extent = list_first_entry(&list, struct extent, lists.ordered);
		r = extent_write_free_header_sync(extent);
		if (r) {
			DMERR("FATAL: Error extent header to %s",
			      cache->dev->name);
			goto err_free;
		}

		hc->disk_header = metadata_alloc(hc);
		BUG_ON(!hc->disk_header);

		r = header_write(hc);
		metadata_free(hc, (void **) &hc->disk_header);
		if (r) {
			DMERR("FATAL: Error writing cache header to %s",
			      cache->dev->name);

			if (grow)
				goto err_free;

			/*
			 * Work any extents to free back
			 * onto the cache's ordered list.
			 */
			list_for_each_entry_safe(extent, tmp, &list,
						 lists.ordered)
				list_move_tail(&extent->lists.ordered,
					      &hc->lists.ordered);

			goto err;
		}
	}

	/*
 	 * Step 3: if growing, work private extent list into init list
 	 *	   and tell worker to start extent initialization again.
 	 * 	   If shrinking, free extents on private lirt.
	 */
	if (grow) {
		/* Move/add new extents to init and ordered list. */
		list_for_each_entry_safe(extent, tmp, &list,
					 lists.ordered) {
			list_move_tail(&extent->lists.ordered,
				       &hc->lists.ordered);
			list_add_tail(&extent->lists.free_init_lru,
				      &hc->lists.init);
			atomic_inc(&hc->extents.total);
		}

		/* Tell worker that there's new extents to initialize. */
		ClearCacheInitialized(hc);
		SetCacheInitializeNew(hc);
		SetCacheDoInitialize(hc);
	} else {
		int first = 1;

	 	/*
		 * Free any extents we shrunk the cache by pulling
		 * them out of work list adjusting counters *but*
		 * the first one listed.
		 */
		list_for_each_entry_safe(extent, tmp, &list,
					 lists.ordered) {
			list_del(&extent->lists.ordered);

			/*
			 * Pull out of hash and free/init/LRU
			 * lists adjusting counters.
			 */
			extent_hash_del(extent);
			extent_free_init_lru_del(extent);

			if (first) {
				list_add_tail(&extent->lists.ordered,
					      &hc->lists.ordered);
				extent_free_init_lru_add(extent);
				first = 0;
				continue;
			}

			if (!ExtentInit(extent))
				atomic_dec(&hc->extents.initialized);

			atomic_dec(&hc->extents.total);
			extent_free(extent);
		}
	}

	DMINFO("%s cache on %s to %llu sectors",
	       grow ? "Grown" : "Shrunk", cache->dev->name,
	       (unsigned long long) cache->size);
out:
	return !!todo;

err_free:
	/* Free any allocated extents on allocation failure. */
	extents_free_list(&list);
	cache->size = cache_size_old;
err:
	return -EINVAL;
}

/* Endio function for dm_io_bio_submit(). */
static void bio_submit_callback(unsigned long error, void *context)
{
	struct bio *bio = context;
	struct extent *extent = (struct extent *) bio->bi_bdev;
	struct hstore_c *hc = extent->hc;
	int write = !!(bio_data_dir(bio) == WRITE);

	/* We've got a bio IO error and flag that on the extent. */
	if (unlikely(error))
		SetExtentError(extent);

	if (write && ExtentMetaIo(extent)) {
		unsigned long flags;

		/*
		 * Need a spinlock here, because endios can
		 * be processed in parallel with my worker.
		 */
		spin_lock_irqsave(&extent->io.endio_lock, flags);
		bio_list_add(&extent->io.endio, bio);
		spin_unlock_irqrestore(&extent->io.endio_lock, flags);

		/* REMOVEME: stats. */
		atomic_inc(&hc->stats.bios_requeued);
	} else {
		/* REMOVEME: stats. */
		atomic_inc(hc->stats.bios_endiod + write);
		bio_endio(bio, error);
		endio_put(extent); /* Drop the reference. */

		if (!write)
			atomic_dec(&hc->io.reads);
	}

	/* To update extent state. */
	extent_endio_add(extent);

	/* Wakeup worker to deal with endio list. */
	wake_do_hstore(hc);
}

/* Asynchronously submit a bio. */
static void dm_io_bio_submit(struct extent *extent, struct bio *bio)
{
	struct dm_io_region region = {
		.bdev = bio->bi_bdev,
		.sector = bio->bi_sector,
		.count = bio_sectors(bio),
	};
	struct dm_io_request control = {
		.bi_rw = bio_data_dir(bio),
		.mem = { .type = DM_IO_BVEC,
			 .ptr.bvec = bio->bi_io_vec + bio->bi_idx },
		.notify = { .fn = bio_submit_callback, .context = bio },
		.client = extent->hc->io.dm_io_client,
	};

	/*
	 * I can use bio->bi_bdev to squirrel the extent reference
	 * through to bio_submit_callback() because dm_io() allocates a new
	 * bio for lower chain submission anyway.
	 */
	bio->bi_bdev = (struct block_device *) extent;
	BUG_ON(dm_io(&control, 1, &region, NULL));
}

/* Copy data across between origin and cache extents. */
static int extent_data_io_async(int rw, struct extent *extent)
{
	struct hstore_c *hc = extent->hc;
	struct dm_io_region cache = {
		.bdev = hc->devs.cache.dev->bdev,
		.sector = extent->addr.cache.offset,
	}, orig = {
		.bdev = hc->devs.orig.dev->bdev,
		.sector = extent->addr.orig.offset,
	}, *from, *to;

	/* REMOVEME: stats. */
	atomic_inc(hc->stats.extent_data_io + !!(rw == WRITE));

	/* Check for partial extent at origin device end. */
	cache.count = orig.count =
	    min(extent_data_size(hc), hc->devs.orig.size - orig.sector);
	/* Set source and destination. */
	rw == READ ? (from = &orig,  to = &cache) :
		     (from = &cache, to = &orig);
	SetCacheCacheIOQueued(hc);
	SetCacheOrigIOQueued(hc);
	return dm_kcopyd_copy(hc->io.kcopyd_client, from, 1, to, 0,
			      kcopy_endio, extent);
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
			if (!TestClearCacheOrigWritable(hc))
				SetCacheOrigWritable(hc);
			r++;
		}
	
		if (TestClearCacheChangeWritePolicy(hc)) {
			if (!TestClearCacheWriteThrough(hc))
				SetCacheWriteThrough(hc);
			r++;
		}
	
		/* If any state changed -> update cache header. */
		/* FIXME: sync write can delay worker execution. */
		if (CachePersistent(hc) && r) {
			hc->disk_header = metadata_alloc(hc);
			BUG_ON(!hc->disk_header);
			header_write(hc);
			metadata_free(hc, (void **) &hc->disk_header);
		}
	}
}

/* IO whole extents (ie. copy data accross between cache and origin). */
static void extent_io_async(int rw, struct extent *extent)
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
		BUG_ON(!extent->disk);
		extent_to_disk(extent);

		/* Restore in core flags. */
		extent->io.flags = flags;

		/* Take out a references for the metadata IOs. */
		endio_get(extent);
	}

	BUG_ON(TestSetExtentCopying(extent));
	BUG_ON(TestSetExtentCopyIo(extent));

	/* Take out a reference for the extent data IOs. */
	endio_get(extent);
	BUG_ON(extent_data_io_async(rw, extent));

	if (CachePersistent(hc))
		BUG_ON(extent_meta_io_async(WRITE, extent));
}

/* Return flush dirty extent flush delay in jiffies. */
static unsigned long dirty_flush_delay(void)
{
	return jiffies + HZ;
}

/* Process bios on an uptodate extent. */
static void _bios_io(int rw, struct extent *extent)
{
	int copy_io, write = !!(rw == WRITE);
	unsigned writes, writes_while_copying;
	struct bio *bio;
	struct bio_list *bios = extent->io.in + write;
	struct hstore_c *hc;

	if (bio_list_empty(bios))
		return;

	hc = extent->hc;
	copy_io = ExtentCopyIo(extent);
	writes = writes_while_copying = 0;

	/*
	 * Remap all queued bios, take out an endio
	 * reference per bio and submit them.
	 */
	while ((bio = bio_list_pop(bios))) {
		/*
		 * Allow reads and writes during extent flushs to
		 * the origin but flag writes to extents being copied
		 * to become redirtied in order to get copied accross
		 * to the original device again.
		 */
		if (write) {
			writes++;

			if (copy_io) {
				/* REMOVEME: stats. */
				atomic_inc(&hc->stats.writes_while_copying);
				writes_while_copying = 1;
			}
		}

		bio_remap(extent, bio);	/* Remap to cache or orig device. */
		endio_get(extent);	/* Take an io reference per bio. */
		dm_io_bio_submit(extent, bio);

		/* REMOVEME: stats. */
		atomic_inc(hc->stats.submitted_io + write);
	}

	/* Dont' throttle on reads. */
	if (!write)
		unplug_cache(hc);

	/*
	 * If I've got writes here *and* the extent hasn't been
	 * dirtied in a previous run -> update metadata on disk.
	 */
	if (writes) {
		int meta_io = 1;

		if (unlikely(CacheWriteThrough(hc))) {
			/* REMOVEME: stats */
			atomic_inc(&hc->stats.extent_clear_uptodate);

			/*
			 * Flag extent not uptodate, because we're
			 * writing through to the original device.
			 */
			if (!TestClearExtentUptodate(extent))
				/* Write metadata only on first change. */
				meta_io = 0;
		} else {
			if (TestSetExtentDirty(extent)) {
				/* Write metadata only on first change. */
				meta_io = 0;

				/* Make sure extent gets redirtied. */
				if (copy_io && writes_while_copying) {
					SetExtentForceDirty(extent);
					/* REMOVEME: stats. */
					atomic_inc(&hc->stats.force_dirty);
				}
			} else {
				extent->io.dirty_expire = dirty_flush_delay();

				/* REMOVEME: stats */
				atomic_inc(&hc->extents.dirty);
			}
		}

		/*
		 * Update metadata to reflect dirty state of this
		 * extent on cache device *once* per state change.
		 */
		if (CachePersistent(hc) && meta_io && !ExtentMetaIo(extent)) {
			extent->disk = metadata_alloc(hc);
			BUG_ON(!extent->disk);
			extent_to_disk(extent);
			/* Take IO reference for metadata write. */
			endio_get(extent);
			BUG_ON(extent_meta_io_async(WRITE, extent));
		}
	}
}

/* Process read before write bios on an uptodate extent. */
static void bios_io(struct extent *extent)
{
	/* Fatal if not uptodate! */
	BUG_ON(!ExtentUptodate(extent));
	_bios_io(READ, extent);
	_bios_io(WRITE, extent);
}

/* Validate extents on cache initialization. */
static void extent_validate(struct extent *extent)
{
	int add_to_free_lru = 1; /* Preset to put extent on free/LRU list. */
	struct hstore_c *hc = extent->hc;

	BUG_ON(!CacheDoInitialize(hc));
	BUG_ON(!TestClearExtentInit(extent));

	/* Suppress error message in case we're expecting to read a free one. */
	if (ExtentError(extent)) {
		if (CacheInitializeNew(hc) ||
		    (ExtentMetaRead(extent) && CacheReadFreeExtent(hc)))
			DMERR_LIMIT("extent=%llu metadata read error",
				    (unsigned long long)
				    extent->addr.cache.offset);
	}

	/*
	 * Metadata read -> insert into hash if used, so that
	 * hash hits for queued bios can start to happen in
	 * do_bios()->_do_bios()->extent_get().
	 */
	if (ExtentMetaRead(extent)) {
		if (ExtentError(extent)) {
			if (CacheReadFreeExtent(hc)) {
				ClearExtentError(extent);
				SetExtentFree(extent);
			}
		} else if (ExtentFree(extent)) {
			/*
			 * A free extent got read and there'll be
			 * only free extents following it by definition ->
			 * we flag that the follwoing ones can be initialized.
			 * by the worker.
			 */
			SetCacheReadFreeExtent(hc);
		} else {
			/*
			 * Insert used extent into hash.
			 * Remove again on error below.
			 */
			extent_hash_insert(extent);

			/*
			 * Put any left behind extents in
			 * need of IO on dirty/flush list.
			 */
			if (!ExtentUptodate(extent)) {
				if (TestClearExtentDirty(extent)) {
					DMERR_LIMIT("extent *not* uptodate "
							"while dirty!");
					extent_hash_del(extent);
				} else {
					add_to_free_lru = 0;
					extent_flush_add(extent);
				}
			} else if (ExtentDirty(extent)) {
				if (!TestSetExtentUptodate(extent)) {
					DMERR_LIMIT("extent dirty while "
						    "*not* uptodate!");
					extent_hash_del(extent);
				} else {
					add_to_free_lru = 0;
					extent->io.dirty_expire =
						dirty_flush_delay();
					extent_dirty_add_sorted(extent);

					/* REMOVEME: stats */
					atomic_inc(&hc->extents.dirty); 
				}
			} /* Else put on LRU list below. */
		}
	}

	/* If not put on dirty/flush list -> add to free/LRU list. */
	if (add_to_free_lru) {
		BUG_ON(!list_empty(&extent->lists.free_init_lru));

		/* We've written it -> trigger move to the free list. */
		if (!ExtentMetaRead(extent))
			SetExtentFree(extent);

		extent_free_init_lru_add(extent);
	}

	ClearExtentMetaRead(extent);

	/* Count as initialized. */
	atomic_inc(&hc->extents.initialized);

	/* Reset cache initialization active after max init extents. */
	if (atomic_dec_and_test(&hc->extents.init_max)) {
		/* ..and stop writing after init_max metadata writes. */
		if (CacheInitializeNew(hc))
			ClearCacheDoInitialize(hc);

		/*
		 * If we read a free extent -> trigger initialization
		 * of any unitialized following it.
		 */
		if (TestClearCacheReadFreeExtent(hc))
			SetCacheInitializeNew(hc);

		ClearCacheInitializationActive(hc);
	}

	/* All extents done. */
	if (extents_initialized(hc) == extents_total(hc)) {
		/* Flag done with extents initialization. */
		SetCacheInitialized(hc);
		ClearCacheInitializationActive(hc);
		ClearCacheDoInitialize(hc);
		ClearCacheInitializeNew(hc);
		DMINFO("initialized %s, %u total/%u free extents",
				hc->devs.cache.dev->name,
				extents_total(hc), extents_free(hc));
	}
}

/* Handle all endios on extents. */
static void do_endios(struct hstore_c *hc)
{
	struct extent *extent;

	while ((extent = extent_endio_pop(hc))) {
		/* Can't go further with active metadata io. */
		if (ExtentMetaIo(extent))
			continue;

		/* If present, a metadata IO has happened. */
		if (extent->disk) {
			endio_put(extent);

			/* Transfer metadata to CPU on read. */
			if (unlikely(ExtentMetaRead(extent))) {
				int error = ExtentError(extent), r;
				sector_t offset = extent->addr.cache.offset;

				BUG_ON(!ExtentInit(extent));

				/*
				 * Need to set flags again, because they're
				 * transient and got overwritten from disk.
				 */
				extent_to_core(extent);

				/* Reset to enable access. */
				ClearExtentCopyIo(extent);
				ClearExtentMetaIo(extent);
				ClearExtentCopying(extent);

				r = extent_check(extent, offset);
				if (r || error) {
					/* Restore in case of error. */
					extent->addr.cache.offset = offset;

					/* Init bogus members. */
					extent->addr.orig.offset = 0;
					SetExtentError(extent);
				}

				/*
				 * Set flag again after read+check,
				 * bacause it's been overwritten.
				 */
				SetExtentMetaRead(extent);
				SetExtentInit(extent);
			}

			/* Free disk header structure. */
			metadata_free(hc, (void **) &extent->disk);

			if (unlikely(ExtentInit(extent))) {
				/* Validate the extent. */
				extent_validate(extent);
				continue;
			}
		}

		/*
		 * From here, we've got bio writes or extent copies.
		 */

		/* End IO *after* the metadata got updated. */
		extent_endio_bio_list(extent);

		/*
		 * Don't update state or put on lru/flush list
		 * until both metadata *and* copy io have finished.
		 */
		if (ExtentCopyIo(extent))
			continue;

		/* REMOVEME: stats. */
		if (endio_ref(extent))
			atomic_inc(&hc->stats.endio_active);

		/* Adjust extent state, if this was an extent copy. */
		if (TestClearExtentCopying(extent)) {
			endio_put(extent);

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

				/* REMOVEME: stats. */
				atomic_dec(&hc->extents.dirty); 
			}
		}

		/* Redirty extent for writes during copy. */
		if (!endio_ref(extent) &&
		    TestClearExtentForceDirty(extent)) {
			SetExtentDirty(extent);
			extent->io.dirty_expire = dirty_flush_delay();

			/* REMOVEME: stats. */
			atomic_inc(&hc->extents.dirty); 
		}

		/*
		 * If there's no new bios to process ->
		 * put on dirty or LRU list, else flush.
		 */
		if (extent_has_bios_queued(extent))
			/* There's bios pending -> put on flush list. */
			extent_flush_add(extent);
		 else if (ExtentDirty(extent))
			extent_dirty_add_sorted(extent);
 		else if (extent_is_idle(extent))
			/* No bios and not dirty -> put on LRU list. */
			extent_free_init_lru_add(extent);
	}
}

/*
 * Initialize extent metadata by either reading off the backing
 * store in case of existing metadata or writing to it in case
 * of a cache initialization or writing of init extents.
 */
static void do_extents_init(struct hstore_c *hc)
{
	if (!CacheDoInitialize(hc) ||
	    CacheInitializationActive(hc))
		return;
	else {
		int i = 0, rw = CacheInitializeNew(hc) ? WRITE : READ;
		struct extent *extent;

		/* Count extents on init list. */
		list_for_each_entry(extent, &hc->lists.init,
			    	lists.free_init_lru) {
			if (++i == PARALLEL_INIT_MAX)
				break;
		}

		atomic_set(&hc->extents.init_max, i);

		while (i && (extent = extent_init_pop(hc))) {
			extent->disk = metadata_alloc(hc);
			BUG_ON(!extent->disk);

			if (rw == WRITE) {
				ClearExtentMetaRead(extent);
				SetExtentFree(extent);
				extent_to_disk(extent);
			} else
				SetExtentMetaRead(extent);

			/* Flag cache is actively initializing. */
			SetCacheInitializationActive(hc);

			/* Take endio reference out and initiate IO. */
			endio_get(extent);
			BUG_ON(extent_meta_io_async(rw, extent));
			i--;
		}

		BUG_ON(i);
	}
}

/* Resize cache on ctr argument request. */
static void do_resize(struct hstore_c *hc)
{
	if (!CacheDoInitialize(hc) && TestClearCacheResize(hc)) {
		int r;

		mutex_lock(&hc->params.resize_lock);
		if (hc->params.cache_requested_size) {
			hc->params.cache_new_size = 
				hc->params.cache_requested_size;
			hc->params.cache_requested_size = 0;
		}

		mutex_unlock(&hc->params.resize_lock);
			
		/* Need to shrink more because too few where idle. */
		r = cache_resize(hc);
		if (r > 0)
			SetCacheResize(hc);
		else
			/* Done with resizing. */
			hc->params.cache_new_size = 0;
	}
}

/* Process list of bios trying to get extents form hash/free/LRU list. */
static int _do_bios(int rw, struct hstore_c *hc)
{
	int r = 0, write = !!(rw == WRITE);
	/*
	 * In case the original device isn't writable
	 * and the cache is full, I got to error the IO.
	 */
	int need_to_error = !CacheOrigWritable(hc) &&
			    !extents_free(hc) &&
			    !CacheDoInitialize(hc);
	struct bio *bio;
	struct bio_list *bios = hc->io.work + write;
	struct extent *extent;

	/* Work all deferred or new bios on work list. */
	while ((bio = bio_list_pop(bios))) {
		/* This is a fatal cache error so don't claim io succeeds. */
		if (CacheDead(hc)) {
			bio_endio(bio, -EIO);
			continue;
		}

		/* Once the barrier is flagged, defer further IO. */
		if (CacheBarrier(hc)) {
			bio_list_add_head(bios, bio);
			r = -EAGAIN;
			break;
		}

		/* Flag a barrier write. */
		if (bio_empty_barrier(bio))
			SetCacheBarrier(hc);

		/* If I can't get one -> put IO off (or error it; see below). */
		extent = extent_get(hc, bio);
		if (extent) {
			/* REMOVEME: stats */
			atomic_inc(hc->stats.hits + write);
	
			/* If extent is errored, error bio here. */
			if (unlikely(ExtentError(extent)))
				bio_endio(bio, -EIO);
			else {
				/*
				 * Put bio on extents input queue
				 * and extent on flush list,
				 */
				if (!write)
					atomic_inc(&hc->io.reads);

				bio_list_add(extent->io.in + write, bio);
				extent_flush_add(extent);
			}
		} else {
			/* REMOVEME: stats */
			atomic_inc(hc->stats.misses + write);

			/*
			 * If we run out of LRU extents but can write
			 * to the original we defer, otherwise we need
			 * to error the IO unless we're initializing still.
			 */
			if (unlikely(need_to_error))
				bio_endio(bio, -EIO);
			else {
				bio_list_add_head(bios, bio);
				r = -ENOMEM;
				break;
			}
		}
	}

	return r;
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
	int i, r;

	/* Any bios to work on? */
	if (!TestClearCacheNewBiosQueued(hc) &&
	    !cache_has_work_bios_queued(hc))
		return;

	/* When handling a barrier -> wait until all inflight ios finish. */
	if (unlikely(CacheBarrier(hc))) {
		if (cache_ios_inflight(hc))
			return;
		else
			ClearCacheBarrier(hc);
	}

	/* Quickly add any new bios queued to the end of the work list. */
	i = ARRAY_SIZE(hc->io.work);
	spin_lock_irq(&hc->io.lock);
	while (i--) {
		bio_list_merge(hc->io.work + i, hc->io.in + i);
		bio_list_init(hc->io.in + i);
	}

	spin_unlock_irq(&hc->io.lock);

	r = _do_bios(READ, hc);
	if (!r)
		_do_bios(WRITE, hc);
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
			bio_list_for_each(bio, extent->io.in + 1)
				sectors += bio_sectors(bio);

			/* Set extent uptodate in case writes cover it fully. */
			if (sectors == extent_data_size(hc)) {
				SetExtentUptodate(extent);

				/* REMOVEME: stats. */
				atomic_inc(&hc->stats.overwrite);
			}
		}
	}
}

/* Calculate maximum parallel dirty flsuhing extents depending on cache load. */
static unsigned dirty_flushing_max(struct hstore_c *hc)
{
	unsigned r = PARALLEL_DIRTY_MAX * extents_dirty(hc) / extents_total(hc);

	return r < PARALLEL_DIRTY_MIN ? PARALLEL_DIRTY_MIN : r;
}

/* Flush out any dirty extents in chunks. */
static void do_dirty(struct hstore_c *hc)
{
	unsigned free = extents_free(hc) + extents_lru(hc);

	/* Can't flush dirty extents to non-writable origin device. */
	if (!CacheOrigWritable(hc) ||
	    CacheInitializationActive(hc) ||
	    (atomic_read(&hc->io.reads) && free) ||
	    !extents_dirty(hc))
		return;
	else {
		unsigned flushing = atomic_read(&hc->extents.dirty_flushing),
			 flushing_max = dirty_flushing_max(hc),
			 free_min = extents_total(hc) / 2,
			 this_flushing = 0;
		struct extent *extent, *tmp;

		if (flushing >= flushing_max)
			return;

		/* Try flushing harder. */
		if (!free)
			flushing_max = PARALLEL_IO_MAX * 4 / 5;

		/* Remove dirty extents from flush list. */
		list_for_each_entry_safe(extent, tmp, &hc->lists.flush,
					 lists.dirty_flush) {
			if (ExtentDirty(extent) &&
			    !extent_has_bios_queued(extent)) {
				/* REMOVEME: stats. */
				atomic_inc(&hc->stats.evict_from_flush);
				extent_dirty_add_sorted(extent);
			}
		}

		/*
		 * Only put a dynamically calculated maximum
		 * of any dirty extents on flush list in case:
		 *
		 *   o the cache is idle
		 *   o and we don't have more than a minimum of free extents
		 */

		hc->io.dirty_expire = ~0;

		/* Submit maximum dirty extents to flush. */
		list_for_each_entry_safe(extent, tmp, &hc->lists.dirty,
					 lists.dirty_flush) {
			/*
			 * Wait until expired unless busy and
			 * store delay for rescheduling to flush
			 * next dirty extent(s).
			 */
			if (free > free_min) {
				unsigned long ede = extent->io.dirty_expire,
					      j = jiffies;

				/* Get schedule to run worker again. */
				if (ede > j) {
					hc->io.dirty_expire = ede - j;
					break;
				}
			}

			if (this_flushing + flushing >= flushing_max)
				break;

			BUG_ON(ExtentCopying(extent));
			BUG_ON(ExtentCopyIo(extent));
			BUG_ON(ExtentMetaIo(extent));
			extent_flush_add(extent);
			this_flushing++;
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

	if (cache_max_ios_inflight(hc) ||
	    list_empty(&hc->lists.flush))
		return;

	/* Sort flush list. */
	sort_flush_list(hc);

	/* Work all extents on flush list. */
	while ((extent = extent_flush_pop(hc))) {
		if (ExtentUptodate(extent)) {
			/*
			 * Extent is uptodate and dirty; we can write
			 * it out to the origin or submit bios.
			 *
			 * Fatal if extent not dirty!
			 */
			if (extent_has_bios_queued(extent)) {
				/*
				 * Submit any bios hanging off
				 * this extent sinput queues.
				 */
				bios_io(extent);
			} else {
				if (ExtentCopying(extent))
					/* REMOVEME: stats. */
					atomic_inc(&hc->stats.dropped_on_flush);
				else {
					BUG_ON(!ExtentDirty(extent));
					/* Write extent out to the origin. */
					extent_io_async(WRITE, extent);
					atomic_inc(&hc->extents.dirty_flushing);

					/* REMOVEME: stats. */
					atomic_inc(&hc->stats.dirty_flushing_total);
				}
			} 
		} else if (!ExtentCopying(extent)) {
			/*
			 * If the extent isn't uptodate ->
			 * read it in and update extent metadata.
			 *
			 * Fatal if extent dirty!
			 */
			BUG_ON(ExtentDirty(extent));
			extent_io_async(READ, extent);
		} else
			/* REMOVEME: stats. */
			atomic_inc(&hc->stats.dropped_on_flush);

		if (cache_max_ios_inflight(hc))
			break;
	}
}

/*
 * Check, if we need to reschedule the worker delayed
 * because there's dirty extents still but no io causing
 * that they will get worked on.
 */
static void do_reschedule(struct hstore_c *hc)
{
	if (!work_pending(&hc->io.ws.work) &&
	    extents_dirty(hc) &&
	    hc->io.dirty_expire < ~0)
		wake_do_hstore_delayed(hc, hc->io.dirty_expire);
}

/* Wake up any waiters in case we're idle. */
static void do_wake(struct hstore_c *hc)
{
	/* Wake up any suspend waiter. */
	if (cache_idle(hc))
		wake_up(&hc->io.suspendq);
}

/*
 * Hierarchical storage worker thread.
 *
 * o do setting changes requested via message interface
 * o handle all outstanding endios on extents
 * o resize cache if requested by constructor/message interface;
 *   do this _after_ processing endios, because that may have
 *   put extents on the LRU list
 * o work on all new queued bios putting them on extent bio queues
 * o initialize any uninitialized extents
 *   (read preexisting in or write free new ones)
 * o check for extents, which get completely written over and
 *   avoid extent reads if not uptodate
 * o add any dirty extents to flush list
 * o flush to recognize any bios and extent io requests and unplug
 *   cache and original devices request queues
 * o check if a reschedule is needed to work on delayed dirty extent flushs
 * o wake any suspend waiters if idle
 */
static void do_hstore(struct work_struct *ws)
{
	struct hstore_c *hc = container_of(ws, struct hstore_c, io.ws.work);
	/* not_suspended = "only may run while _not_ suspended". */
	int not_suspended = !CacheSuspend(hc);

	if (not_suspended)
		 do_settings(hc);

	do_endios(hc);

	if (not_suspended)
		do_resize(hc);

	do_bios(hc);

	if (not_suspended)
		do_extents_init(hc);

	do_overwrite_check(hc);

	if (not_suspended)
		 do_dirty(hc);

	do_flush(hc);
	unplug_cache(hc);

	if (not_suspended)
		do_reschedule(hc);

	do_wake(hc);
}

/*
 * Create or read the cache device header.
 * Write minimum amount of extents on new cache.
 */
enum store_type { TRANSIENT, PERSISTENT };
static int cache_init(struct hstore_c *hc)
{
	int r;
	enum handle_type handle = hc->params.handle;
	struct hstore_c *hc_read;

	/* Read any existing header into temporary hstore_c strcuture. */
	hc_read = kzalloc(sizeof(*hc), GFP_KERNEL);
	if (!hc_read)
		return -ENOMEM;

	*hc_read = *hc;
	hc_read->disk_header = metadata_alloc(hc);
	BUG_ON(!hc_read->disk_header);

	r = header_io_sync(hc_read, READ);
	if (r) {
		DM_ERR("reading cache header from %s",
		       hc->devs.cache.dev->name);
		goto err;
	}

	header_to_core(hc_read);

	/* Found disk header magic but invalid metadata -> WARN and bail out. */
	r = header_check(hc_read);
	if (r == -EPERM) {
		DMWARN("header magic found but header data invalid "
		       "(hstore metadata version invalid?)");
		goto err;
	} else if (!r && !CachePersistent(hc)) {
		DMERR("transient cache but existing header -> exiting");
		r = -EPERM;
		goto err;
	}

	if (handle == CREATE_CACHE || (handle == AUTO_CACHE && r)) {
		if (handle == AUTO_CACHE && hc->params.params < 2) {
			DMERR("need cache size with auto to initialize");
			r = -EINVAL;
			goto err;
		}

		/* Intialize cache header and 1st extent. */
		if (CachePersistent(hc)) {
			struct extent *extent;

			DMINFO("%sriting cache device %s header to %llu "
			       "and free extent to %llu",
			       r ? "w" : "overw",
			       hc->devs.cache.dev->name,
			       (unsigned long long) hc->devs.cache.start,
			       (unsigned long long) extents_start(hc));

			extent = extent_alloc(hc, GFP_KERNEL);
			if (!extent)
				return -ENOMEM;

			extent_init_address(extent, extents_start(hc));
			r = extent_write_free_header_sync(extent);
			extent_free(extent);
			if (r) {
				DMERR("FATAL: Error writing extent "
				      "header to %s at offset %llu",
				      hc->devs.cache.dev->name,
				      (unsigned long long)
				      extent->addr.cache.offset);
				goto err;
			}

			hc->disk_header = metadata_alloc(hc);
			BUG_ON(!hc->disk_header);
			r = header_write(hc);
			metadata_free(hc, (void **) &hc->disk_header);
			if (r) {
				DMERR("writing cache header to %s",
				      hc->devs.cache.dev->name);
				goto err;
			}
	
			/* Flag extent initialization writes. */
			SetCacheInitializeNew(hc);
		} else
			r = 0;
	} else {
		if (r)
			goto err;

		DMINFO("read cache device %s header",
		       hc->devs.cache.dev->name);
		hc->extents.size = hc_read->disk_header->size.extent;
		hc->devs.cache.size = hc_read->disk_header->size.dev;
		hc->io.flags |= hc_read->disk_header->flags;

		/* Flag extent initialization reads. */
		ClearCacheInitializeNew(hc);
	}

	ClearCacheInitialized(hc);
err:
	metadata_free(hc, (void **) &hc_read->disk_header);
	kfree(hc_read);
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

	if (hc->io.wq) {
		flush_workqueue(hc->io.wq);
		cancel_delayed_work(&hc->io.ws);
		destroy_workqueue(hc->io.wq);
	}

	extents_free_list(&hc->lists.init);
	BUG_ON(!list_empty(&hc->lists.init));
	extents_free_list(&hc->lists.lru);
	BUG_ON(!list_empty(&hc->lists.lru));

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
		DM_ERR("Invalid device start sector");

	dev->start = tmp;
	if (dev->start >= dev->size - MIN_CACHE_SIZE)
		DM_ERR("Invalid device start/length");

	r = dm_get_device(ti, argv[0], FMODE_READ | FMODE_WRITE, &dev->dev);
	if (r) {
		DM_ERR_RET(-ENXIO, "Device lookup failed");
	} else {
		/* Check device limits. */
		if (dev->size != check_dev_access(ti->private, dev, dev->size))
			DM_ERR("Device size too small");
	}

	return 0;
}

/* Check helper: device sizes make sense? */
static int size_check(struct hstore_c *hc)
{
	sector_t cache_size, orig_size;
	struct devs *d = &hc->devs;

	orig_size = d->orig.size - d->orig.start;
	cache_size = extents_total(hc) * extent_data_size(hc);
	if (orig_size < cache_size)
		DM_ERR("origin device size smaller than total cache data size");

	if (!multiple(d->cache.start, META_SECTORS))
		DM_ERR("cache offset is not divisable by %llu",
		       (unsigned long long) META_SECTORS);

	if (extents_total(hc) < EXTENTS_MIN)
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
 * Returns index 1..N for strings hit on list and 0 if no hit.
 */
static int str_listed(const char *str, ...)
{
	int r = 0, i = 1;
	const char *s;
	va_list str_list;

	va_start(str_list, str);

	while ((s = va_arg(str_list, const char *))) {
		if (!strnicmp(str, s, strlen(str))) {
			r = i;
			break;
		}

		i++;
	}

	va_end(str_list);
	return r;
}

/* Check origin device read/write parameter */
static int orig_rw(const char *arg)
{

	if (str_listed(arg, "-", NULL))
		return -EEXIST;
	else if (str_listed(arg, "readonly", NULL))
		return READ;
	else if (str_listed(arg, "readwrite", NULL))
		return WRITE;
	else
		return -EINVAL;
}

enum write_type { WRITEBACK, WRITETHROUGH };
static enum write_type cache_write_policy(const char *arg)
{
	if (str_listed(arg, "-", NULL))
		return -EEXIST;
	else if (str_listed(arg, "writeback", NULL))
		return WRITEBACK;
	else if (str_listed(arg, "writethrough", NULL))
		return WRITETHROUGH;
	else
		return -EINVAL;
}

static enum store_type cache_store_policy(const char *arg)
{
	if (str_listed(arg, "-", NULL))
		return -EEXIST;
	else if (str_listed(arg, "persistent", NULL))
		return PERSISTENT;
	else if (str_listed(arg, "transient", NULL))
		return TRANSIENT;
	else
		return -EINVAL;
}

/* Allocate and initialize a hstore_c context. */
static int
context_create(struct dm_target *ti, char **argv,
	       sector_t cache_start, sector_t cache_size, sector_t extent_size,
	       int hstore_params, int orig_access, enum handle_type handle,
	       enum write_type write_policy, enum store_type store_policy)
{
	int i, r;
	/* Contingency reserve for bios. */
	unsigned parallel_io_max = PARALLEL_IO_MAX * 3 / 2;
	sector_t extents;
	struct list_head list;
	struct hstore_c *hc;
	struct extent *extent, *tmp;

	INIT_LIST_HEAD(&list);

	/* Got all constructor information to allocate context. */
	hc = kzalloc(sizeof(*hc), GFP_KERNEL);
	if (!hc)
		TI_ERR_RET(-ENOMEM, "Cannot allocate hstore context");

	/* Save #hstore_params and hstore_params for status output. */
	hc->params.params = hstore_params;
	hc->params.handle = handle;
	hc->params.cache_size = cache_size;
	mutex_init(&hc->params.resize_lock);
	hc->params.extent_size = extent_size;
	hc->params.write_policy = write_policy;
	hc->params.store_policy = store_policy;

	/* Preset extent size. */
	hc->extents.size = extent_size ? extent_size : EXTENT_SECTORS_DEFAULT;

	init_waitqueue_head(&hc->io.suspendq);	/* Suspend waiters. */
	atomic_set(&hc->io.ref, 0);
	atomic_set(&hc->io.reads, 0);
	atomic_set(&hc->extents.free, 0);
	atomic_set(&hc->extents.initialized, 0);
	atomic_set(&hc->extents.lru, 0);
	atomic_set(&hc->extents.total, 0);
	atomic_set(&hc->extents.dirty, 0);
	atomic_set(&hc->extents.dirty_flushing, 0);
	atomic_set(&hc->extents.init_max, 0);

	i = ARRAY_SIZE(hc->io.in);
	while (i--) {
		bio_list_init(hc->io.in + i);
		bio_list_init(hc->io.work + i);
	}

	spin_lock_init(&hc->io.lock);
	spin_lock_init(&hc->lists.lock_endio);
	INIT_LIST_HEAD(&hc->lists.dirty);
	INIT_LIST_HEAD(&hc->lists.endio);
	INIT_LIST_HEAD(&hc->lists.flush);
	INIT_LIST_HEAD(&hc->lists.lru);
	INIT_LIST_HEAD(&hc->lists.free);
	INIT_LIST_HEAD(&hc->lists.init);
	INIT_LIST_HEAD(&hc->lists.ordered);
	ti->private = hc;
	hc->ti = ti;

	/* Create mempool for disk headers. */
	hc->io.metadata_pool =
		mempool_create_kmalloc_pool(parallel_io_max, SECTOR_SIZE);
	if (!hc->io.metadata_pool)
		TI_ERR_RET(-ENOMEM, "Failure allocating memory pool");

	/* Use dm_io to io cache metadata. */
	hc->io.dm_io_client = dm_io_client_create(parallel_io_max / 4);
	if (IS_ERR(hc->io.dm_io_client))
		TI_ERR_RET(PTR_ERR(hc->io.dm_io_client),
			   "Failure creating dm_io client");

	/* Get cache device. */
	hc->devs.cache.size = cache_size ? cache_size :
					   cache_start + META_SECTORS;;
	r = get_dev(ti, argv, &hc->devs.cache);
	if (r)
		TI_ERR_RET(r, "Cache device access error");

	/* Get original (cached) device. */
	hc->devs.orig.size = ti->len;
	r = get_dev(ti, argv + hstore_params + 3, &hc->devs.orig);
	if (r)
		TI_ERR_RET(r, "Origin device access error");

	/* Change cache flags if requested by ctr arguments. */
	hc->params.access = orig_access;

	if (hc->params.access == READ)
		ClearCacheOrigWritable(hc);
	else
		SetCacheOrigWritable(hc);

	if (hc->params.write_policy == WRITEBACK ||
	    hc->params.write_policy == -EEXIST)
		ClearCacheWriteThrough(hc);
	else if (hc->params.write_policy == WRITETHROUGH)
		SetCacheWriteThrough(hc);
	else
		BUG();

	if (hc->params.store_policy == PERSISTENT ||
	    hc->params.store_policy == -EEXIST)
		SetCachePersistent(hc);
	else if (hc->params.store_policy == TRANSIENT)
		ClearCachePersistent(hc);
	else
		BUG();

	/* Initialize the cache header and initial extents if new. */
	r = cache_init(hc);
	if (r)
		TI_ERR_RET(r, "Initializing cache header");

	/*
	 * Try reaquiring the cache device when size
	 * in header differs from ctr parameter.
	 */
	if (cache_size && cache_size != hc->devs.cache.size) {
		sector_t dsize = check_dev_access(hc, &hc->devs.cache, cache_size);

		if (cache_size != dsize)
			TI_ERR_RET(-ENOMEM,
				   "Invalid cache size; device too small");

		/* Flag for worker thread, that cache needs resizing. */
		hc->params.cache_new_size = cache_size;
		SetCacheResize(hc);
	}

	/* Calculate after potential read and set total extents. */
	extents = hc->devs.cache.size - extents_start(hc);
	do_div(extents, extent_total_size(hc));
	atomic_set(&hc->extents.total, extents);

	/* Check, if device sizes and offsets are valid. */
	r = size_check(hc);
	if (r)
		TI_ERR_RET(r, "Cache/origin size check failed");

	/* Calculate amount of kcopyd pages needed to copy extents. */
	do_div(extent_size, to_sector(PAGE_SIZE));

	/* I use kcopyd to copy extents between cache and origin device. */
	r = dm_kcopyd_client_create(PARALLEL_DIRTY_MAX *
				    extent_size / to_sector(PAGE_SIZE),
				    &hc->io.kcopyd_client);
	if (r)
		TI_ERR_RET(-ENOMEM, "Failure creating dm_kcopyd client");

	/* Initialize IO hash */
	r = hash_init(hc);
	if (r)
		TI_ERR_RET(r, "Failure allocating cache extent hash");

	/* Allocate extent structs and put them on init list. */
	r = cache_extents_alloc(hc, hc->devs.cache.size,
				extents_start(hc), extents_total(hc),
				&list, GFP_KERNEL);

	/* Move/add new extents to init and ordered list. */
	list_for_each_entry_safe(extent, tmp, &list,
				 lists.ordered) {
		list_move_tail(&extent->lists.ordered,
			       &hc->lists.ordered);
		list_add_tail(&extent->lists.free_init_lru,
			      &hc->lists.init);
	}

	if (r)
		TI_ERR_RET(r, "Failure allocating cache extents");

	/* Create singlethreaded workqueue for this hstore device. */
	INIT_DELAYED_WORK(&hc->io.ws, do_hstore);
	hc->io.wq = create_singlethread_workqueue(DAEMON);
	if (!hc->io.wq) 
		TI_ERR_RET(-ENOMEM, "Failure creating cache io work queue");

	/* Flag extent initialization needs doing by worker thread. */
	ClearCacheInitializationActive(hc);

	if (CachePersistent(hc)) {
		SetCacheDoInitialize(hc);
		ClearCacheInitialized(hc);
	} else {
		/* Else do it here. */
		struct extent *extent;

		/*
		 * Assume all extents got validated ->
		 * put them onto the free list.
		 */
		while ((extent = extent_init_pop(hc))) {
			ClearExtentInit(extent);
			SetExtentFree(extent);
			extent_free_init_lru_add(extent);
			atomic_inc(&hc->extents.initialized);
		}

		ClearCacheDoInitialize(hc);
		ClearCacheInitializeNew(hc);
		SetCacheInitialized(hc);
	}

	/* Set masks for fast bio -> extent mapping. */
	hc->extents.mask_inv = extent_data_size(hc) - 1;
	hc->extents.mask = ~hc->extents.mask_inv;

	/* No larger bios than the extent size and no boundary crossing. */
	ti->split_io = extent_data_size(hc);
	BUG_ON(!ti->split_io);

	/* REMOVEME: stats. */
	stats_init(&hc->stats);
	SetCacheStatistics(hc);
	return 0;
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
 * params = {auto/create/open} [cache_check_dev_access( [#cache_extent_size \
 *	     [-/readwrite/readonly \
 *	     [-/writeback/writethrough \
 *           [-/persistent/transient]]]]]
 *
 * "-" implies the next option (eg. "-" = "readwrite")
 *
 * 'auto' causes open of a cache with a valid header or
 * creation of a new cache if there's no vaild one sized to
 * cache_check_dev_access(.
 * If 'auto' is being used on a non-existing cache without cache_check_dev_access(
 * or cache_check_dev_access( = 0, the constructor fails.
 *
 * 'create' enforces creation of a new cache with cache_check_dev_access(.
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
 *	       get tried to cache_check_dev_access(. cache_check_dev_access( = 0 doesn't
 *	       change the cache size.
 * 2 + 'create': the cache device will get initialized and sized
 *	         to cache_check_dev_access(.
 * 2 + 'auto': the cache device will either be opened and tried to resize
 * 	       or get initialized and sized to cache_check_dev_access(.
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
			    (tmp && (tmp < META_SECTORS ||
				     tmp < to_sector(PAGE_SIZE) ||
				     tmp > MAX_EXTENT_SIZE)))
				TI_ERR("Invalid cache extent size argument")

			extent_size = tmp;
		}

		/* Get origin rw mode. */
		if (hstore_params > 3) {
			orig_access = orig_rw(argv[6]);
			if (orig_access < 0 && orig_access != -EEXIST)
				TI_ERR("Invalid original rw parameter");
		}

		/* Get cache write policy. */
		if (hstore_params > 4) {
			write_policy = cache_write_policy(argv[7]);
			if (write_policy < 0 && write_policy != -EEXIST)
				TI_ERR("Invalid cache write policy argument");
		}

		/* Get cache persistency policy / invalidation request. */
		if (hstore_params > 5) {
			store_policy = cache_store_policy(argv[8]);
			if (store_policy < 0 && store_policy != -EEXIST)
				TI_ERR("Invalid cache persistency/invalidation "
				       "policy argument");
		}
	}

	/* Got all parameters -> create hstore context. */
	r = context_create(ti, argv, cache_start, cache_size, extent_size,
			   hstore_params, orig_access,
			   handle, write_policy, store_policy);
	if (r)
		hstore_dtr(ti);

	return r;
}

/*
 * Map a cache io by handing it to the worker thread.
 */
static int hstore_map(struct dm_target *ti, struct bio *bio,
		      union map_info *map_context)
{
	int rw;
	struct hstore_c *hc = ti->private;

	/* I don't want to waste cache capacity. */
	if (bio_rw(bio) == READA)
		return -EIO;

	bio->bi_sector -= ti->begin;	/* Remap sector to target begin. */
	rw = !!(bio_data_dir(bio) == WRITE);

	/* Queue bio to the cache and wake up worker thread. */
	spin_lock_irq(&hc->io.lock);
	bio_list_add(hc->io.in + rw, bio);
	spin_unlock_irq(&hc->io.lock);

	/* REMOVEME: stats */
	atomic_inc(hc->stats.io + rw);

	/* Wakeup worker to deal with bio input list. */
	SetCacheNewBiosQueued(hc);
	wake_do_hstore(hc);
	return DM_MAPIO_SUBMITTED;	/* Handle later. */
}

/* Flush method. */
static void hstore_flush(struct dm_target *ti)
{
	struct hstore_c *hc = ti->private;

	flush_workqueue(hc->io.wq);

	/* Wait until all io has been processed. */
	wait_event(hc->io.suspendq, cache_idle(hc));
}

/* Post suspend method. */
static void hstore_postsuspend(struct dm_target *ti)
{
	struct hstore_c *hc = ti->private;

	/* Tell worker thread to stop initiationg new IO. */
	SetCacheSuspend(hc);
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
		if (CacheOrigWritable(hc)) {
			DMWARN("origin is already readwrite.");
			return -EPERM;
		}
	} else if (!CacheOrigWritable(hc)) {
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

	if (sscanf(arg, "%llu", &tmp) != 1 ||
	    tmp < extents_start(hc) + EXTENTS_MIN * extent_total_size(hc)) {
		DMERR("Size smaller than cache minimum size");
		return -EINVAL;
	}

	if (tmp != check_dev_access(hc, &hc->devs.cache, tmp)) {
		DMERR("Invalid cache size; device too small");
		return -EINVAL;
	}

	/* Set requested cache size. */
	mutex_lock(&hc->params.resize_lock);
	hc->params.cache_requested_size = tmp;
	mutex_unlock(&hc->params.resize_lock);

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
	int o = str_listed(arg, "on", "off", "reset", NULL), r = 0;

	if (o == 1)
		SetCacheStatistics(hc);
	else if (o == 2)
		ClearCacheStatistics(hc);
	else if (o == 3)
		stats_init(&hc->stats);
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

/* bvec merge method. */
static int hstore_merge(struct dm_target *ti, struct bvec_merge_data *bvm,
			struct bio_vec *biovec, int max_size)
{
	struct hstore_c *hc = ti->private;
	struct request_queue *q  = bdev_get_queue(hc->devs.cache.dev->bdev);

	/* REMOVEME: stats. */
	atomic_inc(&hc->stats.merge_bvec_fn);

	if (!q->merge_bvec_fn)
		return max_size;

	/*
	 * We can tell from here, if this is to the origin or the cache.
	 *
	 * What we can't tell from here is the sector offset because
	 * that's the workers task to tell on requesting extents.
	 * Best assumption is sector 0.
	 */
	bvm->bi_bdev = (CacheWriteThrough(hc) && bvm->bi_rw == WRITE) ?
		       hc->devs.orig.dev->bdev : hc->devs.cache.dev->bdev;
	bvm->bi_sector = 0;
	return min(max_size, q->merge_bvec_fn(q, bvm, biovec));
}


/* Status output method. */
static int hstore_status(struct dm_target *ti, status_type_t type,
			 char *result, unsigned maxlen)
{
	unsigned dirty;
	ssize_t sz = 0;
	struct hstore_c *hc = ti->private;
	struct stats *s;

	if (!hc)
		return 0;

	switch (type) {
	case STATUSTYPE_INFO:
		s = &hc->stats;
		dirty = extents_dirty(hc);
		DMEMIT("v=%s %llu %s %s %s %s %s %s %s",
		     version,
		     (unsigned long long) hc->devs.cache.size,
		     CacheDoInitialize(hc) ? "INIT" : "ready",
		     CacheOrigWritable(hc) ? "readwrite" : "readonly",
		     CacheWriteThrough(hc) ? "writethrough" : "writeback",
		     CachePersistent(hc) ? "persistent" : "transient",
		     dirty ? "DIRTY" : "clean",
		     dirty > extents_total(hc) * 9 / 10 ?
		     "GROW!" : "ok",
		     hc->params.cache_new_size > hc->devs.cache.size ?
		     "growing" : "shrinking");

		if (CacheStatistics(hc))
			DMEMIT(" es=%llu flgs=0x%lx bkts=%u r=%u/%u w=%u/%u "
			     "h=%u/%u m=%u/%u wwc=%u dof=%u "
			     "fd=%u rd=%u wd=%u rs=%u ws=%u ca=%u dr=%u dw=%u "
			     "mr=%u mw=%u ec=%u ov=%u eioa=%u "
			     "req=%u ef=%u el=%u ei=%u et=%u eh=%u "
			     "eff=%u ed=%u edf=%u edft=%u iof=%u ci=%u mbf=%u",
			     (unsigned long long) extent_data_size(hc),
			     hc->io.flags, hc->hash.buckets,
			     atomic_read(s->io),
			     atomic_read(s->bios_endiod),
			     atomic_read(s->io + 1),
			     atomic_read(s->bios_endiod + 1),
			     atomic_read(s->hits), atomic_read(s->hits + 1),
			     atomic_read(s->misses),
			     atomic_read(s->misses + 1),
			     atomic_read(&s->writes_while_copying),
			     atomic_read(&s->dropped_on_flush),
			     atomic_read(&s->force_dirty),
			     atomic_read(s->deferred_io),
			     atomic_read(s->deferred_io + 1),
			     atomic_read(s->submitted_io),
			     atomic_read(s->submitted_io + 1),
			     atomic_read(&s->extent_copy_active),
			     atomic_read(s->extent_data_io),
			     atomic_read(s->extent_data_io + 1),
			     atomic_read(s->extent_meta_io),
			     atomic_read(s->extent_meta_io + 1),
			     atomic_read(&s->extent_clear_uptodate),
			     atomic_read(&s->overwrite),
			     atomic_read(&s->endio_active),
			     atomic_read(&s->bios_requeued),
			     extents_free(hc),
			     extents_lru(hc),
			     extents_initialized(hc),
			     extents_total(hc),
			     atomic_read(&s->extents_hashed),
			     atomic_read(&s->evict_from_flush),
			     dirty,
			     atomic_read(&hc->extents.dirty_flushing),
			     atomic_read(&s->dirty_flushing_total),
			     cache_ios_inflight(hc),
			     cache_idle(hc),
			     atomic_read(&s->merge_bvec_fn));
		break;

	case STATUSTYPE_TABLE:
		DMEMIT("%s %llu %u",
		       hc->devs.cache.dev->name,
		       (unsigned long long) hc->devs.cache.start,
		       hc->params.params);

		if (hc->params.params)
			DMEMIT(" %s", handle_str(hc->params.handle));

		if (hc->params.params > 1)
			DMEMIT(" %llu",
			       (unsigned long long) hc->params.cache_size);

		if (hc->params.params > 2)
			DMEMIT(" %llu",
			       (unsigned long long) hc->params.extent_size);

		if (hc->params.params > 3) {
			DMEMIT(" %s",
			       hc->params.access == READ ?  "readonly" :
			       (hc->params.access < 0 ? "-" : "readwrite"));
		}

		if (hc->params.params > 4)
			DMEMIT(" %s",
			       hc->params.write_policy == WRITETHROUGH ?
			       "writethrough" :
			       (hc->params.write_policy < 0 ?
				"-" : "writeback"));

		if (hc->params.params > 5)
			DMEMIT(" %s", hc->params.store_policy == TRANSIENT ?
			       "transient" :
			       (hc->params.store_policy < 0 ?
				"-" :  "persistent"));

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
	blk_limits_io_opt(limits, extent_data_size(hc));
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
	.postsuspend = hstore_postsuspend,
	.resume = hstore_resume,
	.message = hstore_message,
	.merge = hstore_merge,
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
