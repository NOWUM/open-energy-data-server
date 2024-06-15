
export function getDataFormat(metadata) {
    const hasTemporal = metadata.temporal_start || metadata.temporal_end;
    const hasSpatial = metadata.concave_hull_geometry;
    if (hasTemporal && hasSpatial) return "Form: Temporal & Spatial";
    if (hasTemporal) return "Form: Temporal";
    if (hasSpatial) return "Form: Spatial";
    return "Form: Unknown";
};