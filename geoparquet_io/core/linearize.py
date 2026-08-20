"""Pure-Python linearization of curved WKB geometries.

DuckDB's spatial extension cannot parse the curved ISO WKB types and
GeoParquet cannot represent them (see ``curved_geometry.py``), so the only
correct conversion is stroking arcs into line segments at ingest — the same
operation OGR performs for ``-nlt CONVERT_TO_LINEAR`` and PostGIS for
``ST_CurveToLine``. This module rewrites curved WKB into its linear
equivalent with the standard library only:

- CIRCULARSTRING (8)  -> LINESTRING (2)
- COMPOUNDCURVE (9)   -> LINESTRING (2)
- CURVEPOLYGON (10)   -> POLYGON (3)
- MULTICURVE (11)     -> MULTILINESTRING (5)
- MULTISURFACE (12)   -> MULTIPOLYGON (6)

Linear geometries pass through byte-identical (except inside a
GEOMETRYCOLLECTION whose children changed, which is reserialized). Arcs are
sampled at a maximum angular step (default 4 degrees, matching GDAL's
``OGR_ARC_STEPSIZE`` default); arc endpoints are emitted exactly, and Z/M
values interpolate linearly along the arc parameter. Collinear "arcs"
degrade to straight segments.
"""

from __future__ import annotations

import math
import struct

#: Default maximum angular step per emitted segment, in degrees (OGR default).
DEFAULT_MAX_ANGLE_DEG = 4.0

_LINESTRING = 2
_POLYGON = 3
_MULTILINESTRING = 5
_MULTIPOLYGON = 6
_GEOMETRYCOLLECTION = 7
_CIRCULARSTRING = 8
_COMPOUNDCURVE = 9
_CURVEPOLYGON = 10
_MULTICURVE = 11
_MULTISURFACE = 12

_CURVED = {_CIRCULARSTRING, _COMPOUNDCURVE, _CURVEPOLYGON, _MULTICURVE, _MULTISURFACE}

_REMAP = {
    _CIRCULARSTRING: _LINESTRING,
    _COMPOUNDCURVE: _LINESTRING,
    _CURVEPOLYGON: _POLYGON,
    _MULTICURVE: _MULTILINESTRING,
    _MULTISURFACE: _MULTIPOLYGON,
}


class LinearizeError(ValueError):
    """Raised when a WKB blob cannot be linearized."""


class _Reader:
    def __init__(self, buf: bytes) -> None:
        self.buf = buf
        self.pos = 0

    def byte(self) -> int:
        v = self.buf[self.pos]
        self.pos += 1
        return v

    def uint32(self, le: bool) -> int:
        (v,) = struct.unpack_from("<I" if le else ">I", self.buf, self.pos)
        self.pos += 4
        return int(v)

    def doubles(self, n: int, le: bool) -> tuple[float, ...]:
        vals = struct.unpack_from(("<" if le else ">") + "d" * n, self.buf, self.pos)
        self.pos += 8 * n
        return vals


def _decode_type(code: int) -> tuple[int, bool, bool]:
    """Return (base_type, has_z, has_m) for an ISO WKB type code."""
    if code & 0xE0000000:
        raise LinearizeError(f"EWKB flags are not supported (type code 0x{code:08x})")
    base = code % 1000
    band = code // 1000
    if band == 0:
        return base, False, False
    if band == 1:
        return base, True, False
    if band == 2:
        return base, False, True
    if band == 3:
        return base, True, True
    raise LinearizeError(f"Unrecognized WKB type code {code}")


def _type_code(base: int, has_z: bool, has_m: bool) -> int:
    return base + (1000 if has_z else 0) + (2000 if has_m else 0)


def _stroke_arc(
    p0: tuple[float, ...],
    p1: tuple[float, ...],
    p2: tuple[float, ...],
    max_angle_rad: float,
) -> list[tuple[float, ...]]:
    """Vertices approximating the circular arc p0->p1->p2, excluding p0.

    Extra ordinates (Z/M) are interpolated linearly along the sweep. A
    degenerate (collinear or zero-radius) arc falls back to straight segments
    through the control points.
    """
    (x0, y0), (x1, y1), (x2, y2) = p0[:2], p1[:2], p2[:2]

    # Circumcenter via perpendicular bisectors.
    d = 2.0 * (x0 * (y1 - y2) + x1 * (y2 - y0) + x2 * (y0 - y1))
    if abs(d) < 1e-12:
        return [p1, p2]  # collinear: straight lines through the control points
    s0 = x0 * x0 + y0 * y0
    s1 = x1 * x1 + y1 * y1
    s2 = x2 * x2 + y2 * y2
    cx = (s0 * (y1 - y2) + s1 * (y2 - y0) + s2 * (y0 - y1)) / d
    cy = (s0 * (x2 - x1) + s1 * (x0 - x2) + s2 * (x1 - x0)) / d
    radius = math.hypot(x0 - cx, y0 - cy)
    if radius < 1e-12:
        return [p1, p2]

    a0 = math.atan2(y0 - cy, x0 - cx)
    a1 = math.atan2(y1 - cy, x1 - cx)
    a2 = math.atan2(y2 - cy, x2 - cx)

    # Choose the sweep direction that passes through the middle control point.
    ccw_mid = (a1 - a0) % (2.0 * math.pi)
    ccw_end = (a2 - a0) % (2.0 * math.pi)
    if ccw_end == 0.0:
        ccw_end = 2.0 * math.pi  # closed circle described by three points
    if ccw_mid <= ccw_end:
        sweep = ccw_end
    else:
        sweep = ccw_end - 2.0 * math.pi  # clockwise

    steps = max(2, math.ceil(abs(sweep) / max_angle_rad))
    extras0, extras2 = p0[2:], p2[2:]
    out: list[tuple[float, ...]] = []
    for i in range(1, steps + 1):
        t = i / steps
        if i == steps:
            out.append(p2)  # exact endpoint
            continue
        angle = a0 + sweep * t
        extras = tuple(e0 + (e2 - e0) * t for e0, e2 in zip(extras0, extras2, strict=True))
        out.append((cx + radius * math.cos(angle), cy + radius * math.sin(angle), *extras))
    return out


class _Linearizer:
    def __init__(self, max_angle_deg: float) -> None:
        self.max_angle_rad = math.radians(max_angle_deg)
        self.changed = False

    # ---- parsing helpers -------------------------------------------------
    def _points(self, r: _Reader, le: bool, dims: int) -> list[tuple[float, ...]]:
        n = r.uint32(le)
        return [r.doubles(dims, le) for _ in range(n)]

    def _curve_to_points(self, r: _Reader, has_z: bool, has_m: bool) -> list[tuple[float, ...]]:
        """Read one curve component (LINESTRING/CIRCULARSTRING/COMPOUNDCURVE)
        from its own WKB header and return its linearized vertex list."""
        le = r.byte() == 1
        base, z, m = _decode_type(r.uint32(le))
        if (z, m) != (has_z, has_m):
            raise LinearizeError("Mixed Z/M dimensions inside a curve are not supported")
        dims = 2 + z + m
        if base == _LINESTRING:
            return self._points(r, le, dims)
        if base == _CIRCULARSTRING:
            self.changed = True
            pts = self._points(r, le, dims)
            return self._stroke_circularstring(pts)
        if base == _COMPOUNDCURVE:
            self.changed = True
            out: list[tuple[float, ...]] = []
            for _ in range(r.uint32(le)):
                seg = self._curve_to_points(r, has_z, has_m)
                if out and seg and out[-1] == seg[0]:
                    seg = seg[1:]  # segments share their joint vertex
                out.extend(seg)
            return out
        raise LinearizeError(f"Unexpected curve component type {base}")

    def _stroke_circularstring(self, pts: list[tuple[float, ...]]) -> list[tuple[float, ...]]:
        if len(pts) < 3 or len(pts) % 2 == 0:
            raise LinearizeError(f"CIRCULARSTRING needs an odd point count >= 3, got {len(pts)}")
        out = [pts[0]]
        for i in range(0, len(pts) - 2, 2):
            out.extend(_stroke_arc(pts[i], pts[i + 1], pts[i + 2], self.max_angle_rad))
        return out

    # ---- serialization ---------------------------------------------------
    def _write_geometry(self, r: _Reader, out: bytearray) -> None:
        """Read one geometry from ``r`` and append its linear WKB to ``out``."""
        le = r.byte() == 1
        code = r.uint32(le)
        base, has_z, has_m = _decode_type(code)
        dims = 2 + has_z + has_m

        def header(new_base: int) -> None:
            out.append(1)  # emit little-endian
            out.extend(struct.pack("<I", _type_code(new_base, has_z, has_m)))

        def write_ring(ring: list[tuple[float, ...]]) -> None:
            out.extend(struct.pack("<I", len(ring)))
            for pt in ring:
                out.extend(struct.pack("<" + "d" * dims, *pt))

        if base in (1, _LINESTRING, _POLYGON, 4, _MULTILINESTRING, _MULTIPOLYGON):
            # Linear: re-emit verbatim (normalized to little-endian).
            header(base)
            if base == 1:  # POINT
                out.extend(struct.pack("<" + "d" * dims, *r.doubles(dims, le)))
            elif base == _LINESTRING:
                write_ring(self._points(r, le, dims))
            elif base == _POLYGON:
                n_rings = r.uint32(le)
                out.extend(struct.pack("<I", n_rings))
                for _ in range(n_rings):
                    write_ring(self._points(r, le, dims))
            else:  # MULTIPOINT / MULTILINESTRING / MULTIPOLYGON: nested WKB
                n = r.uint32(le)
                out.extend(struct.pack("<I", n))
                for _ in range(n):
                    self._write_geometry(r, out)
        elif base == _GEOMETRYCOLLECTION:
            header(_GEOMETRYCOLLECTION)
            n = r.uint32(le)
            out.extend(struct.pack("<I", n))
            for _ in range(n):
                self._write_geometry(r, out)
        elif base == _CIRCULARSTRING:
            self.changed = True
            pts = self._stroke_circularstring(self._points(r, le, dims))
            header(_LINESTRING)
            write_ring(pts)
        elif base == _COMPOUNDCURVE:
            self.changed = True
            segments: list[tuple[float, ...]] = []
            for _ in range(r.uint32(le)):
                seg = self._curve_to_points(r, has_z, has_m)
                if segments and seg and segments[-1] == seg[0]:
                    seg = seg[1:]
                segments.extend(seg)
            header(_LINESTRING)
            write_ring(segments)
        elif base == _CURVEPOLYGON:
            self.changed = True
            n_rings = r.uint32(le)
            rings = [self._curve_to_points(r, has_z, has_m) for _ in range(n_rings)]
            header(_POLYGON)
            out.extend(struct.pack("<I", len(rings)))
            for ring in rings:
                if ring and ring[0] != ring[-1]:
                    ring = [*ring, ring[0]]  # rings must close
                write_ring(ring)
        elif base in (_MULTICURVE, _MULTISURFACE):
            self.changed = True
            header(_REMAP[base])
            n = r.uint32(le)
            out.extend(struct.pack("<I", n))
            for _ in range(n):
                self._write_geometry(r, out)
        else:
            raise LinearizeError(f"WKB type {base} cannot be linearized")


def linearize_wkb(
    wkb: bytes | bytearray | memoryview,
    max_angle_deg: float = DEFAULT_MAX_ANGLE_DEG,
) -> tuple[bytes, bool]:
    """Return ``(linear_wkb, changed)`` for one ISO WKB geometry.

    ``changed`` is False when the input contained no curved types (the output
    is then a little-endian re-emission of the same geometry). Raises
    :class:`LinearizeError` for WKB that cannot be linearized (EWKB flags,
    the surface family POLYHEDRALSURFACE/TIN/TRIANGLE, malformed input).
    """
    lin = _Linearizer(max_angle_deg)
    out = bytearray()
    try:
        lin._write_geometry(_Reader(bytes(wkb)), out)
    except (struct.error, IndexError) as e:
        raise LinearizeError(f"Malformed WKB: {e}") from e
    return bytes(out), lin.changed


def contains_curved_wkb(wkb: bytes | bytearray | memoryview) -> bool:
    """Cheap top-level check: does this WKB's outermost type belong to the
    curved family? (Curves nested inside a GEOMETRYCOLLECTION are found by
    :func:`linearize_wkb` itself.)"""
    b = bytes(wkb[:5])
    if len(b) < 5:
        return False
    le = b[0] == 1
    (code,) = struct.unpack("<I" if le else ">I", b[1:5])
    try:
        base, _, _ = _decode_type(code)
    except LinearizeError:
        return False
    return base in _CURVED or base == _GEOMETRYCOLLECTION
