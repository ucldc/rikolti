import os
import subprocess
import shutil
from settings import CONTENT_PROCESSES

class UnsupportedMimetype(Exception):
    pass


class ThumbnailError(Exception):
    pass


# Settings recommended as a starting point by Jon Stroop.
# See https://groups.google.com/forum/?hl=en#!searchin/iiif-discuss/kdu_compress/iiif-discuss/OFzWFLaWVsE/wF2HaykHcd0J
KDU_COMPRESS_BASE_OPTS = [
    "-quiet", "-rate",
    "2.4,1.48331273,.91673033,.56657224,.35016049,.21641118,.13374944,"
    ".08266171",
    "Creversible=yes", "Clevels=7", "Cblk={64,64}", "Cuse_sop=yes",
    "Cuse_eph=yes", "Corder=RLCP", "ORGgen_plt=yes", "ORGtparts=R",
    "Stiles={1024,1024}", "-double_buffering", "10", "-num_threads", "4",
    "-no_weights"
]

KDU_COMPRESS_DEFAULT_OPTS = KDU_COMPRESS_BASE_OPTS[:]
KDU_COMPRESS_DEFAULT_OPTS.extend(["-jp2_space", "sRGB"])


# decorator function
def subprocess_exception_handler(func):
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except subprocess.CalledProcessError as e:
            print(
                f"{func.__name__} command failed: {e.cmd}\n"
                f"returncode was: {e.returncode}\n"
                f"output was: {e.output}"
            )
            return None
    return wrapper


def check_thumb_mimetype(mimetype):
    if mimetype not in ['image/jpeg', 'application/pdf', 'video/mp4']:
        raise UnsupportedMimetype(f"thumbnail: {mimetype}")


def make_thumbnail(source_file_path, mimetype):
    '''
        generate thumbnail image for PDF, video, or image
    '''
    check_thumb_mimetype(mimetype)

    thumbnail = None
    if mimetype == 'image/jpeg':
        thumbnail = source_file_path
    if mimetype == 'application/pdf':
        thumbnail = pdf_to_thumb(source_file_path)
    if mimetype == 'video/mp4':
        thumbnail = video_to_thumb(source_file_path)

    return thumbnail


@subprocess_exception_handler
def pdf_to_thumb(pdf_file_path):
    '''
        generate thumbnail image for PDF
        use ImageMagick `convert` tool as described here:
        http://www.nuxeo.com/blog/qa-friday-thumbnails-pdf-psd-documents/
    '''
    thumb_file_path = f"{pdf_file_path.split('.')[0]}.png"
    if os.path.exists(thumb_file_path):
        return thumb_file_path

    magick_location = CONTENT_PROCESSES['magick']
    process = [
        magick_location, "-quiet", "-strip", "-format", "png", "-quality",
        "75", f"{pdf_file_path}[0]", thumb_file_path
    ]
    subprocess.check_output(process, stderr=subprocess.STDOUT)
    print("Used ImageMagic `convert` {pdf_file_path} to {thumb_file_path}")
    return thumb_file_path


@subprocess_exception_handler
def video_to_thumb(video_path):
    '''
        generate thumbnail image for video
        use ffmpeg to grab center frame from video
    '''
    thumb_path = f"{video_path.split('.')[0]}.png"
    if os.path.exists(thumb_path):
        return thumb_path

    ffprobe_location = CONTENT_PROCESSES['ffprobe']
    duration_proc = [
        ffprobe_location, '-v', 'fatal', '-show_entries', 'format=duration',
        '-of', 'default=nw=1:nk=1', video_path
    ]

    # get duration of video
    duration = subprocess.check_output(duration_proc, stderr=subprocess.STDOUT)

    # calculate midpoint of video
    midpoint = float(duration.strip()) / 2
    ffmpeg_location = CONTENT_PROCESSES['ffmpeg']

    process = [
        ffmpeg_location, '-v', 'fatal', '-ss', str(midpoint), '-i', video_path,
        '-vframes', '1', thumb_path
    ]

    subprocess.check_output(process, stderr=subprocess.STDOUT)
    print("Used ffmpeg to convert {video_path} to {thumb_path}")
    return thumb_path


def check_mimetype(mimetype):
    ''' do a basic pre-check on the object to see if we think it's
    something know how to deal with '''
    valid_types = ['image/jpeg', 'image/gif', 'image/tiff', 'image/png', 'image/jp2', 'image/jpx', 'image/jpm']

    # see if we recognize this mime type
    if mimetype in valid_types:
        print(
            f"Mime-type '{mimetype}' was pre-checked and recognized as "
            "something we can try to convert."
        )
    elif mimetype in ['application/pdf']:
        raise UnsupportedMimetype(
            f"Mime-type '{mimetype}' was pre-checked and recognized as "
            "something we don't want to convert."
        )
    else:
        raise UnsupportedMimetype(
            f"Mime-type '{mimetype}' was unrecognized. We don't know how to "
            "deal with this"
        )


@subprocess_exception_handler
def tiff_conversion(input_path):
    '''
        convert file using ImageMagick `convert`:
        http://www.imagemagick.org/script/convert.php
    '''
    output_path = f"{input_path.split('.')[0]}.tif"
    if os.path.exists(output_path):
        return output_path

    magick_location = CONTENT_PROCESSES['magick']
    process = [
        magick_location, "-compress", "None",
        "-quality", "100", "-auto-orient", input_path, output_path
    ]
    msg = (
        f"Used ImagMagick convert to convert {input_path} "
        f"to {output_path}"
    )
    subprocess.check_output(process, stderr=subprocess.STDOUT)
    print(msg)
    return output_path


@subprocess_exception_handler
def tiff_to_srgb_libtiff(input_path):
    '''
    convert color profile to sRGB using libtiff's `tiff2rgba` tool
    '''
    output_path = f"{input_path.split('.')[0]}.tiff"
    if os.path.exists(output_path):
        return output_path

    tiff2rgba_location = CONTENT_PROCESSES['tiff2rgba']
    process = [tiff2rgba_location, "-c", "none", input_path, output_path]
    msg = (
        f"Used tiff2rgba to convert {input_path} to {output_path}, "
        "with color profile sRGB (if not already sRGB)"
    )
    subprocess.check_output(process, stderr=subprocess.STDOUT)
    print(msg)
    return output_path


# only used internally to this module
# @subprocess_exception_handler
# def uncompress_jp2000(compressed_path):
#     ''' uncompress a jp2000 file using kdu_expand '''
#     output_path = f"{compressed_path.split('.')[0]}.tiff"
#     if os.path.exists(output_path):
#         return output_path

#     kdu_expand_location = CONTENT_PROCESSES['kdu_expand']
#     process = [
#         kdu_expand_location, "-i", compressed_path, "-o", output_path
#     ]
#     msg = (
#         f"File uncompressed using kdu_expand. Input: "
#         f"{compressed_path}, output: {output_path}"
#     )
#     subprocess.check_output(process, stderr=subprocess.STDOUT)
#     print(msg)
#     return output_path


# only used internally to this module
@subprocess_exception_handler
def tiff_to_jp2(tiff_path):
    ''' convert a tiff to jp2 using kdu_compress.
    tiff must be uncompressed.'''
    jp2_path = f"{tiff_path.split('.')[0]}.jp2"
    if os.path.exists(jp2_path):
        return jp2_path

    # kdu_compress_location = CONTENT_PROCESSES['kdu_compress']
    # process = [kdu_compress_location, "-i", tiff_path, "-o", jp2_path]
    # process.extend(KDU_COMPRESS_DEFAULT_OPTS)

    magick_location = CONTENT_PROCESSES['magick']
    process = [
        magick_location, "-quiet", "-format", "-jp2", "-define", 
        f"jp2:rate=10", f"{tiff_path}[0]", jp2_path
    ]
    msg = "{tiff_path} converted to {jp2_path}"

    # try:
    subprocess.check_output(process, stderr=subprocess.STDOUT)
    print(msg)
    # except subprocess.CalledProcessError as e:
        # print(f"A kdu_compress command failed. Trying alternate:\n{e}")
        # proc = [kdu_compress_location, "-i", tiff_path, "-o", jp2_path]
        # proc.extend(KDU_COMPRESS_BASE_OPTS)
        # try:
        #     subprocess.check_output(process, stderr=subprocess.STDOUT)
        #     print(msg)
        # except subprocess.CalledProcessError as e:
        #     print(
        #         f"ERROR: `kdu_compress` failed: {e.cmd}\n"
        #         f"returncode was: {e.returncode}\n"
        #         f"output was: {e.output}"
        #     )
        #     return None
    return jp2_path


def make_jp2(source_file_path, mimetype):
    print(f"make jp2: {source_file_path}")
    try:
        check_mimetype(mimetype)
    except UnsupportedMimetype as e:
        print(e)
        return source_file_path

    prepped_file_path = None
    if mimetype in ['image/jpeg', 'image/gif', 'image/png', 'image/tiff']:
        converted_file_path = tiff_conversion(source_file_path)
        if converted_file_path:
            prepped_file_path = tiff_to_srgb_libtiff(converted_file_path)
    # elif mimetype in ['image/jp2', 'image/jpx', 'image/jpm']:
    #     prepped_file_path = uncompress_jp2000(source_file_path)

    if not prepped_file_path:
        print(
            f"Didn't know how to prep file with mimetype {mimetype} for "
            "jp2 conversion"
        )
        return

    jp2_filepath = tiff_to_jp2(prepped_file_path)
    # if not jp2_filepath:
    #     shutil.rmtree(tmp_dir)
    #     return

    return jp2_filepath
