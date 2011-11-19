#!/usr/bin/python
# MBUtil2: convert and compress
# run it with -h argument to see help

import os, sys
import sqlite3
import argparse

def check_mbtiles(filename):
    try:
	con = sqlite3.connect(filename)
	con.close()
	return True
    except Exception:
	return False

def copy_mbtiles(mbfrom, mbto):
    print "Copy MBTiles from {0} to {1}".format(mbfrom, mbto)
    confrom = sqlite3.connect(mbfrom)
    curfrom = confrom.cursor();
    con = sqlite3.connect(mbto)
    cur = con.cursor();
    cur.execute("""PRAGMA synchronous=0""")
    cur.execute("""PRAGMA locking_mode=EXCLUSIVE""")
    cur.execute("""create table tiles (zoom_level integer, tile_column integer, tile_row integer, tile_data blob);""")
    cur.execute("""create table metadata (name text, value text);""")
    cur.execute("""create unique index name on metadata (name);""")
    cur.execute("""create unique index tile_index on tiles (zoom_level, tile_column, tile_row);""")
    curfrom.execute("select name, value from metadata")
    for row in curfrom:
	cur.execute("insert into metadata (name, value) values (?, ?)", (row[0], row[1]))
    count = 0
    curfrom.execute("select zoom_level, tile_column, tile_row, tile_data from tiles")
    for row in curfrom:
	cur.execute("insert into tiles (zoom_level, tile_column, tile_row, tile_data) values (?, ?, ?, ?)", (row[0], row[1], row[2], row[3]))
	count = count + 1
	if not count%10000:
	    print "done", count, "tiles"
    cur.execute("ANALYZE;")
    cur.execute("VACUUM;")
    curfrom.close()
    cur.close()
    confrom.close()
    con.close()

def mbtiles_to_dir(filename, directory, tms):
    print "MBTiles {0} to directory {1}".format(filename, directory)
    con = sqlite3.connect(filename)
    cur = con.cursor()
    res = cur.execute("select value from metadata where name='format'").fetchone()
    ext = res[0] if res else 'png'
    if ext == 'jpeg':
	ext = 'jpg'
    count = cur.execute("select count(zoom_level) from tiles").fetchone()[0]
    done = 0
    tiles = cur.execute("select zoom_level, tile_column, tile_row, tile_data from tiles")
    t = tiles.fetchone()
    while t:
	z = t[0]
	x = t[1]
	y = t[2]
	if not tms:
	    y = 2**z - 1 - y
	tile_dir = os.path.join(directory, str(z), str(x))
	if not os.path.isdir(tile_dir):
	    os.makedirs(tile_dir)
	tile = os.path.join(tile_dir, '{0}.{1}'.format(y, ext))
	f = open(tile, 'wb')
	f.write(t[3])
	f.close()
	done = done + 1
	if not done % 1000:
	    print "done", 100*done/count, '%'
	t = tiles.fetchone()
    cur.close()
    con.close()

def dir_to_mbtiles(directory, filename, tms):
    print "Directory {0} to MBTiles {1}".format(directory, filename)
    con = sqlite3.connect(filename)
    cur = con.cursor()
    cur.execute("""PRAGMA synchronous=0""")
    cur.execute("""PRAGMA locking_mode=EXCLUSIVE""")
    cur.execute("""create table if not exists tiles (zoom_level integer, tile_column integer, tile_row integer, tile_data blob);""")
    cur.execute("""create table if not exists metadata (name text, value text);""")
    cur.execute("""create unique index if not exists name on metadata (name);""")
    cur.execute("""create unique index if not exists tile_index on tiles (zoom_level, tile_column, tile_row);""")
    # todo: options
    metadata = [ ('name', directory), ('type', 'overlay' if False else 'baselayer'), ('version', '1') ]
    for name, value in metadata:
	cur.execute('insert or replace into metadata (name, value) values (?, ?)', (name, value))
    # todo: read format if exists and put it into fmt

    fmt = False
    count = 0
    for r1, zlist, f1 in os.walk(directory):
	for z in zlist:
	    for r2, xlist, f2 in os.walk(os.path.join(r1, z)):
		for x in xlist:
		    for r2, f3, ylist in os.walk(os.path.join(r1, z, x)):
			for y in ylist:
			    accept = False
			    if fmt:
				accept = y.endswith(fmt)
			    else:
				if y.endswith('.png') or y.endswith('.jpg') or y.endswith('.jpeg'):
				    fmt = y[-4:]
				    metafmt = 'png' if y.endswith('png') else 'jpg'
				    cur.execute("insert into metadata (name,value) values ('format', ?)", (metafmt,))
				    accept = True
			    if accept:
				f = open(os.path.join(r1, z, x, y), 'rb')
				ry = y.split('.')[0] if tms else 2**int(z) - 1 - int(y.split('.')[0])
				cur.execute("insert or replace into tiles (zoom_level, tile_column, tile_row, tile_data) values (?, ?, ?, ?)", (z, x, ry, sqlite3.Binary(f.read())))
				f.close()
				count = count + 1
				if not count % 1000:
				    print "done", count, "tiles"

    cur.execute("ANALYZE;")
    cur.execute("VACUUM;")
    cur.close()
    con.close()

def compress_mbtiles(filename):
    print "Compress MBTiles file {0}".format(filename)
    size_before = os.path.getsize(filename)
    print "Before:", size_before/1024/1024, "MB"
    con = sqlite3.connect(filename)
    cur = con.cursor()

    # step 0: prepare db
    cur.execute("""PRAGMA synchronous=0""")
    cur.execute("""PRAGMA locking_mode=EXCLUSIVE""")
    cur.execute("CREATE TABLE if not exists images (tile_data blob, tile_id integer)")
    cur.execute("CREATE TABLE if not exists map (zoom_level integer, tile_column integer, tile_row integer, tile_id integer)")

    # step 1: get recurrent tiles
    cur.execute("select tile_data, count(1) as c from tiles where length(tile_data)<1000 group by tile_data order by c desc limit 50")
    cache = [];
    repl_tiles = 0
    res = cur.fetchall()
    for row in res:
	if row[1] > 1:
	    cur.execute("insert into images(tile_data, tile_id) values (?, ?)", (row[0], len(cache)))
	    cache.append(row[0])
	    repl_tiles = repl_tiles + row[1]

    # step 2: remap all tiles
    cur.execute("select count(zoom_level) from tiles")
    total = cur.fetchone()[0]
    print "Repeating tiles: {0} ({1}%)".format(repl_tiles, 100*repl_tiles/total)
    count = 0
    imidx = len(cache)
    cur.execute("select zoom_level, tile_column, tile_row, tile_data from tiles")
    cur2 = con.cursor()
    for row in cur:
	if row[3] in cache:
	    idx = cache.index(row[3])
	else:
	    cur2.execute("insert into images (tile_data, tile_id) values (?, ?)", (row[3], imidx))
	    idx = imidx
	    imidx = imidx + 1
	cur2.execute("insert into map (zoom_level, tile_column, tile_row, tile_id) values (?, ?, ?, ?)", (row[0], row[1], row[2], idx))
	count = count + 1
	if not count%10000:
	    print "done", (100*count)/total, "%"
    cur2.close()
    con.commit()

    # step 3: replace old table with a view
    cur.execute("DROP TABLE tiles")
    cur.execute("CREATE VIEW tiles as SELECT map.zoom_level as zoom_level, map.tile_column as tile_column, map.tile_row as tile_row, images.tile_data as tile_data FROM map JOIN images on images.tile_id = map.tile_id")
    cur.execute("CREATE UNIQUE INDEX map_index on map (zoom_level, tile_column, tile_row)")
    cur.execute("CREATE UNIQUE INDEX images_id on images (tile_id)")
    cur.execute("VACUUM")
    cur.execute("ANALYZE")
    cur.close()
    con.close()
    size_after = os.path.getsize(filename)
    print "After: {0} MB ({1}%)".format(size_after/1024/1024, 100*size_after/size_before)

def mbtiles_info(filename):
    con = sqlite3.connect(filename)
    cur = con.cursor()
    cur.execute("select name, value from metadata")
    for row in cur:
	print "{0}: {1}".format(row[0], row[1].encode('ascii', 'replace'))
    cur.execute("select count(zoom_level) from tiles")
    print "total tiles:", cur.fetchone()[0]
    cur.execute("select distinct zoom_level from tiles order by zoom_level")
    zooms = []
    for row in cur:
	zooms.append(row[0])
    print "zooms:", zooms
    cur.execute("select type from sqlite_master where name='tiles'")
    result = cur.fetchone()
    print "tiles are:", result[0]
    cur.close()
    con.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Convert and compress MBTiles")
    parser.add_argument('input', help='input mbtiles file or directory of tiles')
    parser.add_argument('output', nargs='?', help='destination: directory or file name (shows information for input if omitted)')
    parser.add_argument('-c', '--compress', action='store_true', help='compress mbtiles', default=False)
    parser.add_argument('-t', '--tms', action='store_true', help='tiles in directory are in TMS order', default=False)
    options = parser.parse_args()

    if os.path.isfile(options.input):
	if not check_mbtiles(options.input):
	    print "Input file is not mbtiles"
	    sys.exit(1)
	if options.output:
	    if os.path.isfile(options.output):
		print "File already exists"
	    elif os.path.isdir(options.output) or not os.path.exists(options.output):
		if not os.path.exists(options.output) and options.output.endswith(".mbtiles"):
		    copy_mbtiles(options.input, options.output)
		else:
		    mbtiles_to_dir(options.input, options.output, options.tms)
	    else:
		print "???"
	elif options.compress:
	    compress_mbtiles(options.input)
	else:
	    mbtiles_info(options.input)
    elif os.path.isdir(options.input):
	if options.output:
	    if os.path.isdir(options.output):
		print "Use cp -r!"
	    elif os.path.isfile(options.output):
		print "Updating mbtiles is not supported, sorry."
	    elif not os.path.exists(options.output):
		dir_to_mbtiles(options.input, options.output, options.tms)
		if options.compress:
		    compress_mbtiles(options.output)
	    else:
		print "???"
	else:
	    print "No compression or info for directories, sorry."
    
