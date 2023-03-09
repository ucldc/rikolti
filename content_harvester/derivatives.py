import os
import subprocess
import shutil
from settings import CONTENT_PROCESSES

class UnsupportedMimetype(Exception):
    pass


class ThumbnailError(Exception):
    pass


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

# only used internally to this module
@subprocess_exception_handler
def src_to_jp2(tiff_path):
    ''' convert a tiff to jp2 using image magick.'''
    jp2_path = f"{tiff_path.split('.')[0]}.jp2"
    if os.path.exists(jp2_path):
        return jp2_path

    magick_location = CONTENT_PROCESSES['magick']
    process = [
        magick_location, "-quiet", "-format", "-jp2", "-define",
        "jp2:rate=10", f"{tiff_path}[0]", jp2_path
    ]
    msg = f"{tiff_path} converted to {jp2_path}"

    subprocess.check_output(process, stderr=subprocess.STDOUT)
    print(msg)
    return jp2_path


def make_jp2(source_file_path, mimetype):
    print(f"make jp2: {source_file_path}")
    try:
        check_mimetype(mimetype)
    except UnsupportedMimetype as e:
        print(e)
        return source_file_path

    jp2_filepath = src_to_jp2(source_file_path)

    # If tiff to jp2 was a no-op, don't delete.
    if (jp2_filepath != source_file_path):
        os.remove(source_file_path)

    return jp2_filepath
