# this is copied directly from dpla-ingestion for the OAC Mapper
# we should really get off this methodology. Far prefer explicit
# definition rather than this defensive magic.

PATH_DELIM = '/'


def exists(obj, path):
    """
    Returns True if the key path exists in the object
    """
    found = False
    try:
        found = getprop(obj, path, keyErrorAsNone=False) is not None
    except KeyError:
        pass
    except TypeError:
        pass

    return found


def getprop(obj, path, keyErrorAsNone=False):
    """
    Returns the value of the key identified by interpreting
    the path as a delimited hierarchy of keys
    """
    if '/' not in path:
        if keyErrorAsNone:
            return obj.get(path)
        else:
            return obj[path]

    pp, pn = tuple(path.lstrip(PATH_DELIM).split(PATH_DELIM, 1))
    if pp not in obj:
        if not keyErrorAsNone:
            raise KeyError('Path not found in object: %s (%s)' % (path, pp))
        else:
            return None

    return getprop(obj[pp], pn, keyErrorAsNone)


def iterify(iterable):
    """Treat iterating over a single item or an iterator seamlessly"""
    if (isinstance(iterable, str) or isinstance(iterable, dict)):
        iterable = [iterable]
    try:
        iter(iterable)
    except TypeError:
        iterable = [iterable]

    return iterable
