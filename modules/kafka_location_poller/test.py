from shapely.geometry import Point
from shapely.wkb import dumps
import binascii

# Coordinates
latitude = 35.058564
longitude = -106.5721845

# Create Point with (longitude, latitude)
point = Point(longitude, latitude)

# Convert to WKB and then to hex string
wkb_hex = binascii.hexlify(dumps(point, hex=False)).decode("utf-8").upper()

print(wkb_hex)