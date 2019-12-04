class Image(object): 


# https://github.com/pytroll/trollimage/blob/master/trollimage/image.py

    def check_image_format(fformat):
    """Check that *fformat* is valid
    """
    cases = {"jpg": "jpeg",
             "jpeg": "jpeg",
             "tif": "tiff",
             "tiff": "tif",
             "pgm": "ppm",
             "pbm": "ppm",
             "ppm": "ppm",
             "bmp": "bmp",
             "dib": "bmp",
             "gif": "gif",
             "im": "im",
             "pcx": "pcx",
             "png": "png",
             "xbm": "xbm",
             "xpm": "xpm",
             'jp2': 'jp2',
             }
    fformat = fformat.lower()
    try:
        fformat = cases[fformat]
    except KeyError:
        raise UnknownImageFormat("Unknown image format '%s'." % fformat)
    return fformat