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
    "Manuscript": "Manuscripts",
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