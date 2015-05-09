#!/usr/bin/python
# coding: utf-8

from collections import OrderedDict
import csv
import json
import re
import urllib
import urllib2



### CONSTANTS ###
# Files
IN = 'data/scrobbles.tsv'
OUT = 'scrobbles.ttl'
N = '\n'


# TSV columns
TSV_TIME_ISO = 0 # 2015-01-18T20:57:06 (T is a delimiter)
TSV_TIME_UNIX = 1 # 1421614626
TSV_TRACK = 2 # Bing Bada Bang
TSV_TRACK_MBID = 3 # 66c49cfb-c426-4e8c-9430-845cd76bacbb
TSV_ARTIST = 4 # Futumani
TSV_ARTIST_MBID = 5 # fe58d045-816c-48c8-8090-1c756ab115b2
TSV_ALBUM = 10 # ∞ EP
TSV_ALBUM_MBID = 11 # b05da54f-4649-4a4a-8e56-8d353a44741a
TSV_ALBUM_ARTIST = 12 # Futumani
TSV_ALBUM_ARTIST_MBID = 13 # fe58d045-816c-48c8-8090-1c756ab115b2
TSV_APPLICATION = 14 # Last.fm Scrobbler


# URIs
DBPO = 'dbpo'
DBPR = 'dbpr'
DC = 'dc'
EO = 'eo'
ER = 'er'
FOAF = 'foaf'
LAST = 'last'
MBALBUM = 'mbalbum'
MBARTIST = 'mbartist'
MBTRACK = 'mbtrack'
MO = 'mo'
XSD = 'xsd'

PREFIXES = {
	DBPO: 'http://dbpedia.org/ontology/',
	DC: 'http://purl.org/dc/elements/1.1/',
	EO: 'http://ernes7a.lt/ont/',
	FOAF: 'http://xmlns.com/foaf/0.1/',
	LAST: 'http://purl.org/ontology/last-fm/',
	MO: 'http://purl.org/ontology/mo/',
	XSD: 'http://www.w3.org/2001/XMLSchema#'
}

RESOURCES = {
	DBPR: 'http://dbpedia.org/resource/',
	ER: 'http://ernes7a.lt/sws/',
	MBALBUM: 'http://musicbrainz.org/release/',
	MBARTIST: 'http://musicbrainz.org/artist/',
	MBTRACK: 'http://musicbrainz.org/recording/'
}



# Templates
LITERAL = '"{0:s}"'
LOCATOR = '<{0:s}{1:s}>'
LOOKUP = 'http://lookup.dbpedia.org/api/search/KeywordSearch?{0:s}&{1:s}&{2:s}'
PREFIX = '@prefix {0:s}: <{1:s}> .'
QNAME = '{0:s}:{1:s}'
TRIPLE = '{0:s} {1:s} {2:s} .'


# Types
DATETIME = '^^' + QNAME.format(XSD, 'dateTime')


# Vocabularies
# Classes
ALBUM = 'Record'
APPLICATION = 'Software'
ARTIST = 'MusicArtist'
SCROBBLE = 'ScrobbleEvent'
TRACK = 'Track'

# Properties
P_MEDIA = 'computingMedia'
P_DATE = 'date'
P_MAKER = 'maker'
P_NAME = 'name'
P_TITLE = 'title'
P_TRACK = 'track'
P_TYPE = 'a'

ALBUM_PROPS = [
	P_TYPE,
	QNAME.format(DC, P_TITLE),
	QNAME.format(FOAF, P_MAKER),
	QNAME.format(MO, P_TRACK)
]

APPLICATION_PROPS = [
	P_TYPE,
	QNAME.format(DC, P_TITLE)
]

ARTIST_PROPS = [
	P_TYPE,
	QNAME.format(FOAF, P_NAME)
]

SCROBBLE_PROPS = [
	P_TYPE,
	QNAME.format(DC, P_DATE),
	QNAME.format(EO, P_TRACK),
	QNAME.format(DBPO, P_MEDIA)
]

TRACK_PROPS = [
	P_TYPE,
	QNAME.format(DC, P_TITLE),
	QNAME.format(FOAF, P_MAKER)
]

LOOKUP_MAP = {
	APPLICATION: ['Software', 'Website'],
	MBALBUM: ['Album', 'MusicalWork'],
	MBARTIST: ['Agent'],
	MBTRACK: ['Song', 'Single', 'MusicalWork']
}



### ATTRIBUTES ###
scrobbles = OrderedDict()

albums = {}
artists = {}
applications = {}
tracks = {}

albumMap = {}
artistMap = {}
applicationMap = {}
trackMap = {}



### FUNCTIONS ###
# Extracts, discovers or generates a URI for a given entity.
def getURI(name, URI, type, map):
	if not name:
		return ''
	elif URI:
		return LOCATOR.format(RESOURCES[type], URI)
	elif name in map:
		return map[name]
	else:
		URI = makeURI(name, LOOKUP_MAP[type])
		map[name] = URI
		return URI


# Discovers or generates a URI for a given entity.
def makeURI(name, types):
	URI = discoverURI(name, types)

	if URI:
		URI = LOCATOR.format(RESOURCES[DBPR], URI[28 : ])
	else:
		URI = LOCATOR.format(RESOURCES[ER], generateURI(name))

	return URI


# Discovers a URI for a given entity using DBpedia's Lookup Service.
def discoverURI(entity, types, hits = 1):
	URI = ''
	
	# Drops everything between () and []
	entity = re.sub(r'(\(|\[)[^)]*(\)|\])', '', entity).strip()
	
	# Starts with the most restrictive class to reduce the number of false
	# positives.
	for type in types:
		URL = LOOKUP.format(
			urllib.urlencode({'QueryString': entity}),
			urllib.urlencode({'QueryClass': type}),
			urllib.urlencode({'MaxHits': hits})
		)
	
		request = urllib2.Request(URL, headers = {'Accept' : 'application/json'})
		response = json.load(urllib2.urlopen(request))['results']
		
		if len(response) > 0:
			return response[0]['uri']

	return URI


# Generates a URI for a given entity by spliting the name of the entity by
# replacing whitespace with underscores.
def generateURI(entity):
	return urllib.quote('_'.join(entity.split()))


# Saves a disctionary to a file.
def writeDictionary(out, dictionary, properties):
	for key, values in dictionary.iteritems():
		for n, value in enumerate(values):
			if value:
				# Albums have a list of tracks
				if isinstance(value, list):
					for v in value:
						out.write(TRIPLE.format(key, properties[n], v) + N)
				else:
					out.write(TRIPLE.format(key, properties[n], value) + N)

	out.write(N)



### FLOW ###
with open(IN, 'rb') as tsv:
	# Reads the file, skips the header line
	tsv = csv.reader(tsv, delimiter = '\t')
	next(tsv)
	
	# Loops through the entire file building data structures
	for n, row in enumerate(tsv, start = 1):
		# Retrieves all relevant values from the current row
		isoTime = LITERAL.format(row[TSV_TIME_ISO]) + DATETIME
		trackName = LITERAL.format(row[TSV_TRACK].replace('"', ''))
		trackURI = getURI(row[TSV_TRACK].replace('"', ''), row[TSV_TRACK_MBID], MBTRACK, trackMap)
		artistName = LITERAL.format(row[TSV_ARTIST].replace('"', ''))
		artistURI = getURI(row[TSV_ARTIST].replace('"', ''), row[TSV_ARTIST_MBID], MBARTIST, artistMap)
		albumName = LITERAL.format(row[TSV_ALBUM].replace('"', ''))
		albumURI = getURI(row[TSV_ALBUM].replace('"', ''), row[TSV_ALBUM_MBID], MBALBUM, albumMap)
		albumArtistName = LITERAL.format(row[TSV_ALBUM_ARTIST].replace('"', ''))
		albumArtistURI = getURI(row[TSV_ALBUM_ARTIST].replace('"', ''), row[TSV_ALBUM_ARTIST_MBID], MBARTIST, artistMap)
		applicationName = LITERAL.format(row[TSV_APPLICATION].replace('"', ''))
		applicationURI = getURI(row[TSV_APPLICATION].replace('"', ''), '', APPLICATION, applicationMap)
		
		### Scrobble ###
		scrobbles[LOCATOR.format(RESOURCES[ER], str(n))] = [
			# :145105 a last:ScrobbleEvent .
			QNAME.format(LAST, SCROBBLE),
			# :145105 dc:date '2015-01-18T20:57:06'^^xsd:dateTime .
			isoTime,
			# :145105 eo:track mbtrack:66c49cfb-c426-4e8c-9430-845cd76bacbb .
			trackURI,
			# :145105 dbpo:computingMedia dbpr:Last.fm .
			applicationURI
		]
		
		### Application ###
		if applicationURI:
			applications[applicationURI] = [
				# dbpr:Last.fm a dbpo:Software .
				QNAME.format(DBPO, APPLICATION),
				# dbpr:Last.fm dc:title "Last.fm Scrobber" .
				applicationName
			]
		
		
		### Track ###
		if trackURI:
			tracks[trackURI] = [
				# mbtrack:c8c827f7-7e41-41b6-9777-e62e9ec81395 a mo:Track .
				QNAME.format(MO, TRACK),
				# mbtrack:c8c827f7-7e41-41b6-9777-e62e9ec81395 dc:title "I Said Yes" .
				trackName,
				# mbtrack:c8c827f7-7e41-41b6-9777-e62e9ec81395 foaf:maker mbartist:79a68eda-2f67-45af-a7b4-328d73544dc8 .
				artistURI
			]
		
		### Artist ###
		if artistURI:
			artists[artistURI] = [
				# mbartist:79a68eda-2f67-45af-a7b4-328d73544dc8 a mo:MusicArtist .
				QNAME.format(MO, ARTIST),
				# mbartist:79a68eda-2f67-45af-a7b4-328d73544dc8 foaf:name "Chris Remo" .
				artistName
			]

		### Album ###
		albumTracks = []

		if albumURI:
			if albumURI in albums:
				albumTracks = albums[albumURI][3]
			
			if trackURI:
				if trackURI not in albumTracks:
					albumTracks.append(trackURI)
					albumTracks.sort()

			albums[albumURI] = [
				# mbalbum:f6f31b7b-53eb-4b81-a326-363bebbf02b9 a mo:Record .
				QNAME.format(MO, ALBUM),
				# mbalbum:f6f31b7b-53eb-4b81-a326-363bebbf02b9 dc:title "Gone Home: Original Soundtrack" .
				albumName,
				# mbalbum:f6f31b7b-53eb-4b81-a326-363bebbf02b9 foaf:maker mbartist:79a68eda-2f67-45af-a7b4-328d73544dc8
				albumArtistURI,
				# mbalbum:f6f31b7b-53eb-4b81-a326-363bebbf02b9 mo:track mbtrack:c8c827f7-7e41-41b6-9777-e62e9ec81395 .
				albumTracks
			]
				
		### Album Artist ###
		if albumArtistURI:
			artists[albumArtistURI] = [
				# mbartist:79a68eda-2f67-45af-a7b4-328d73544dc8 a mo:MusicArtist .
				QNAME.format(MO, ARTIST),
				# mbartist:79a68eda-2f67-45af-a7b4-328d73544dc8 foaf:name "Chris Remo" .
				albumArtistName
			]

# Sorts the data structures to increase readability of the output file.
applications = OrderedDict(sorted(applications.iteritems(), key = lambda x: x[0].lower()))
tracks = OrderedDict(sorted(tracks.iteritems()))
artists = OrderedDict(sorted(artists.iteritems()))
albums = OrderedDict(sorted(albums.iteritems()))


with open(OUT, 'wb') as ttl:
	# Writes prefixes
	PREFIXES = OrderedDict(sorted(PREFIXES.iteritems(), key = lambda x: x[0]))
	for PREF, URI in PREFIXES.iteritems():
		ttl.write(PREFIX.format(PREF, URI) + N)

	# Writes an empty line
	ttl.write(N)

	# Writes scrobbles
	writeDictionary(ttl, scrobbles, SCROBBLE_PROPS)

	# Writes applications
	writeDictionary(ttl, applications, APPLICATION_PROPS)

	# Writes tracks
	writeDictionary(ttl, tracks, TRACK_PROPS)

	# Writes artists
	writeDictionary(ttl, artists, ARTIST_PROPS)

	# Writes albums
	writeDictionary(ttl, albums, ALBUM_PROPS)