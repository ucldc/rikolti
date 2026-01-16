# These constants are used in the enrichment chain
rights_status = {   # repeating self here, how to get from avram?
    'CR': 'copyrighted',
    'PD': 'public domain',
    'UN': 'copyright unknown',
    # 'X':  'rights status unknown', # or should throw error?
}

rights_statement_default = (
    "Please contact the contributing institution for "
    "more information regarding the copyright status "
    "of this object."
)

dcmi_types = {
    'C': 'Collection',
    'D': 'Dataset',
    'E': 'Event',
    'I': 'Image',
    'F': 'Moving Image',
    'R': 'Interactive Resource',
    'V': 'Service',
    'S': 'Software',
    'A': 'Sound',   # A for audio
    'T': 'Text',
    'P': 'Physical Object',
    # 'X': 'type unknown' # default, not set
}
move_date_value_reg_search = [
    (
        r"\d{1,4}\s*[-/]\s*\d{1,4}\s*[-/]\s*\d{1,4}\s*[-/]\s*\d{1,4}\s*[-/]\s*"
        r"\d{1,4}\s*[-/]\s*\d{1,4}"
    ),
    r"\d{1,2}\s*[-/]\s*\d{4}\s*[-/]\s*\d{1,2}\s*[-/]\s*\d{4}",
    r"\d{4}\s*[-/]\s*\d{1,2}\s*[-/]\s*\d{4}\s*[-/]\s*\d{1,2}",
    r"\d{1,4}\s*[-/]\s*\d{1,4}\s*[-/]\s*\d{1,4}",
    r"\d{4}\s*[-/]\s*\d{4}",
    r"\d{1,2}\s*[-/]\s*\d{4}",
    r"\d{4}\s*[-/]\s*\d{1,2}",
    r"\d{4}s?",
    r"\d{1,2}\s*(?:st|nd|rd|th)\s*century",
    r".*circa.*",
    r".*[pP]eriod(?!.)"
]
scdl_fix_format = {
    "Pamphlet": "Pamphlets",
    "Pamplet": "Pamphlets",
    "Pamplets": "Pamphlets",
    "Pamplets\n": "Pamphlets",
    "pamphlets": "Pamphlets",
    "Manuscript": "Manuscripts",
    "manuscripts": "Manuscripts",
    "Photograph": "Photographs",
    "Baskets (Containers)": "Baskets (containers)",
    "color print": "Color prints",
    "color prints": "Color prints",
    "Image": "Images",
    "Masks (costume)": "Masks (costumes)",
    "Newspaper": "Newspapers",
    "Object": "Objects",
    "Still image": "Still images",
    "Still Image": "Still images",
    "StillImage": "Still images",
    "Text\nText": "Texts",
    "Text": "Texts",
    "Batik": "Batiks",
    "Book": "Books",
    "Map": "Maps",
    "maps": "Maps",
    "Picture Postcards": "Picture postcards",
    "Religous objects": "Religious objects",
    "Tray": "Trays",
    "Wall hanging": "Wall hangings",
    "Wood carving": "Wood carvings",
    "Woodcarving": "Wood carvings",
    "Cartoons (humourous images)": "Cartoons (humorous images)",
    "black-and-white photographs": "Black-and-white photographs"
}

# type_map and format_map were previously a psuedo-hierarchical list of
# tuple-mappings with a hierarchy dependent on the order of the tuples.
# enrich_type() would iterate through the list to see if
# tuple[0] *in* record_type and return the first match. I've changed this to an
# explicit exact match: if record_type == key then return value. I've also
# changed the data structure here to a dict to reflect this change. Finally,
# I've added some work to the enrich_type() method to strip any trailing 's'
# characters (which was the only use case I could see for the psuedo-hierarchy)
type_map = {
    "photographs": "image",
    "photograph": "image",
    "photographic print": "image",    # fix for islandora 
    "sample book": "image",
    "ambrotype": "image",
    "carte-de-visite": "image",
    "daguerreotype": "image",
    "cyanotype": "image",
    "card, collecting": "image",
    "card, souvenir": "image",
    "application/pdf": "text",
    "application/msword": "text",
    "book": "text",
    "booklet": "text",
    "document": "text",
    "documents": "text",
    "articles (documents)": "text",
    "label, product": "image",
    "specimen": "image",
    "electronic resource": "interactive resource",
    "software": "interactive resource",
    "textile": "image",
    "text": "text",
    "texts": "text",
    "frame": "image",
    "costume": "image",
    "object": "physical object",
    "physical object": "physical object",
    "statue": "image",
    "sculpture": "image",
    "container": "image",
    "jewelry": "image",
    "furnishing": "image",
    "furniture": "image",
    "moving image": "moving image",
    "movingimage": "moving image",
    "image": "image",
    "images": "image",
    "stillimage": "image",
    "still image": "image",
    "stil image": "image", # fix for 26725
    "negative": "image",
    "slide": "image",
    "drawing": "image",
    "map": "image",
    "print": "image",
    "painting": "image",
    "illumination": "image",
    "poster": "image",
    "appliance": "image",
    "tool": "image",
    "electronic component": "image",
    "publication": "text",
    "magazine": "text",
    "journal": "text",
    "pamphlet": "text",
    "newsletter": "text",
    "newspaper": "text",
    "essay": "text",
    "transcript": "text",
    "program": "text",
    "program notes": "text",
    "music, sheet": "text",
    "schedule": "text",
    "postcard": "image",
    "correspondence": "text",
    "writing": "text",
    "manuscript": "text",
    "yearbook": "text", # for for pspl 26715
    "scrapbook": "text", # for quartex 23760
    "school yearbook": "text", # for islandora 26601
    "equipment": "image",
    "cartographic": "image",
    "notated music": "image",
    "mixed material": ['image', 'text'],
    "audio": "sound",
    "sound": "sound",
    "oral history recording": "sound",
    "finding aid": "collection",
    "online collection": "collection",
    "online exhibit": "interactive resource",
    "motion picture": "moving image",
    "movie": "moving image",
    "movies": "moving image",
    "cellulose nitrate film": "image",  # fix for UCLA
    "nitrate film": "image",  # fix for UCLA
    "film": "moving image",
    "video game": "interactive resource",
    "video": "moving image",
    "audio/x-wav": "sound",
    "image/jpeg": "image",
    "video/mp4": "moving image",
    "video/quicktime": "moving image",
    "video mp4 file": "moving image",
    "sound mp3 file": "sound",
    "streaming media": "interactive resource",
    "sound recording": "sound",
    "http://id.loc.gov/vocabulary/resourceTypes/art": "physical object",  # fix for UCLA
    "http://id.loc.gov/vocabulary/resourceTypes/aud": "sound",
    "http://id.loc.gov/vocabulary/resourceTypes/car": "image",
    "http://id.loc.gov/vocabulary/resourceTypes/col": "collection",
    "http://id.loc.gov/vocabulary/resourceTypes/dat": "dataset",
    "http://id.loc.gov/vocabulary/resourceTypes/man": "text",
    "http://id.loc.gov/vocabulary/resourceTypes/mix": "collection",
    "http://id.loc.gov/vocabulary/resourceTypes/mov": "moving image",
    "http://id.loc.gov/vocabulary/resourceTypes/mul": "interactive resource",
    "http://id.loc.gov/vocabulary/resourceTypes/not": "text",
    "http://id.loc.gov/vocabulary/resourceTypes/img": "image",
    "http://id.loc.gov/vocabulary/resourceTypes/tac": "physical object",
    "http://id.loc.gov/vocabulary/resourceTypes/txt": "text"
}
format_map = {
    "holiday card": "image",
    "christmas card": "image",
    "mail art": "image",
    "postcard": "image",
    "image": "image",
    "still image": "image",
    "photographic postcard": "image" # fix for 27845 quartex
}
imt_types = ['application', 'audio', 'image', 'message', 'model',
             'multipart', 'text', 'video']
