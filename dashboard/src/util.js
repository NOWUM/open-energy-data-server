
export function getDataFormat(metadata) {
    const hasTemporal = metadata.temporal_start || metadata.temporal_end;
    const hasSpatial = metadata.concave_hull_geometry;
    if (hasTemporal && hasSpatial) return "Temporal & Spatial";
    if (hasTemporal) return "Temporal";
    if (hasSpatial) return "Spatial";
    return "Relational";
};