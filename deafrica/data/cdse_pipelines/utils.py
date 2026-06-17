import math

def snap_bbox_to_grid(bbox, origin_x, origin_y, dx, dy):
    minx, miny, maxx, maxy = bbox
    i0 = math.floor((minx - origin_x) / dx)
    i1 = math.ceil((maxx - origin_x) / dx)
    minx_s = origin_x + i0 * dx
    maxx_s = origin_x + i1 * dx

    j0 = math.floor((origin_y - maxy) / dy)
    j1 = math.ceil((origin_y - miny) / dy)
    maxy_s = origin_y - j0 * dy
    miny_s = origin_y - j1 * dy

    width = int(round((maxx_s - minx_s) / dx))
    height = int(round((maxy_s - miny_s) / dy))

    return [minx_s, miny_s, maxx_s, maxy_s], width, height

def load_evalscript(path):
    with open(path) as f:
        return f.read()
