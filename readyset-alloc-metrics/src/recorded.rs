/// Gauge: Total number of bytes allocated by the application.
pub const ALLOCATED_BYTES: &str = "readyset_allocator_allocated_bytes";

/// Gauge: Total number of bytes in active pages allocated by the application.
pub const ACTIVE_BYTES: &str = "readyset_allocator_active_bytes";

/// Gauge: Total number of bytes dedicated to metadata.
pub const METADATA_BYTES: &str = "readyset_allocator_metadata_bytes";

/// Gauge: Maximum number of bytes in physically resident data pages
/// mapped by the allocator.
pub const RESIDENT_BYTES: &str = "readyset_allocator_resident_bytes";

/// Gauge: Total number of bytes in chunks mapped on behalf of the application.
pub const MAPPED_BYTES: &str = "readyset_allocator_mapped_bytes";

/// Gauge: Total number of bytes in virtual memory mappings that were retained
/// rather than being returned to the operating system.
pub const RETAINED_BYTES: &str = "readyset_allocator_retained_bytes";

/// Gauge: Total number of bytes that are resident but not "active" or "metadata".
pub const DIRTY_BYTES: &str = "readyset_allocator_dirty_bytes";

/// Gauge: Total number of bytes that are in active pages but are not "allocated"
/// by the process.
pub const FRAGMENTED_BYTES: &str = "readyset_allocator_fragmented_bytes";
