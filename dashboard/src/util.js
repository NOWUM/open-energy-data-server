
export function getDataFormat(metadata) {
    const hasTemporal = metadata.temporal_start || metadata.temporal_end;
    const hasSpatial = metadata.concave_hull_geometry;
    if (hasTemporal && hasSpatial) return "Temporal & Spatial";
    if (hasTemporal) return "Temporal";
    if (hasSpatial) return "Spatial";
    return "Relational";
};

export function getFormattedSize(size) {
    if (size < 1000) return `${size} B`;
    if (size < 1000 * 1000) return `${(size / 1024).toFixed(2)} KB`;
    if (size < 1000 * 1000 * 1000) return `${(size / (1000 * 1000)).toFixed(2)} MB`;
    if (size < 1000 * 1000 * 1000 * 1000) return `${(size / (1000 * 1000 * 1000)).toFixed(2)} GB`;
    return `${(size / (1000 * 1000 * 1000 * 1000)).toFixed(2)} TB`;
};