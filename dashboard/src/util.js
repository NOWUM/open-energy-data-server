
export function getDataFormat(metadata) {
    const hasTemporal = metadata.temporal_start || metadata.temporal_end;
    const hasSpatial = metadata.concave_hull_geometry;
    if (hasTemporal && hasSpatial) return "Type: Temporal & Spatial";
    if (hasTemporal) return "Type: Temporal";
    if (hasSpatial) return "Type: Spatial";
    return "Type: Standard";
};