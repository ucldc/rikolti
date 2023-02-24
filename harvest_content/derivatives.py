import os
import subprocess
import shutil

class UnsupportedMimetype(Exception):
    pass


class ThumbnailError(Exception):
    pass


def make_thumb_from_pdf(pdf_file_path, thumb_file_path):
    '''
        generate thumbnail image for PDF
        use ImageMagick `convert` tool as described here:
        http://www.nuxeo.com/blog/qa-friday-thumbnails-pdf-psd-documents/
    '''
    if os.path.exists(thumb_file_path):
        print(f"File already exists, won't overwrite: {thumb_file_path}")
        return True
    try:
        subprocess.check_output(
            [
                os.environ.get(
                    'PATH_MAGICK_CONVERT',
                    '/usr/local/bin/convert'
                ),
                "-quiet",
                "-strip",
                "-format",
                "png",
                "-quality",
                "75",
                pdf_file_path,
                thumb_file_path
            ],
            stderr=subprocess.STDOUT
        )
        print(
            "Used ImageMagic `convert` to convert "
            f"{pdf_file_path} to {thumb_file_path}"
        )
        return True
    except subprocess.CalledProcessError as e:
        print(
            f"ERROR: ImageMagic `convert` command failed: {e.cmd}\n"
            f"returncode was: {e.returncode}\n"
            f"output was: {e.output}"
        )
        return False


def make_thumb_from_video(video_path, thumb_path):
    '''
        generate thumbnail image for video
        use ffmpeg to grab center frame from video
    '''
    # get duration of video
    if os.path.exists(thumb_path):
        print(f"File already exists, won't overwrite: {thumb_path}")
        return True
    try:
        duration = subprocess.check_output(
            [
                os.environ.get(
                    'PATH_FFPROBE',
                    '/usr/local/bin/ffprobe'
                ),
                '-v',
                'fatal',
                '-show_entries',
                'format=duration',
                '-of',
                'default=nw=1:nk=1',
                video_path
            ],
            stderr=subprocess.STDOUT
        )
    except subprocess.CalledProcessError as e:
        print(
            f"ERROR: ffprobe check failed: {e.cmd}\n"
            "returncode was: {e.returncode}\n"
            "output was: {e.output}"
        )
        return False

    # calculate midpoint of video
    midpoint = float(duration.strip()) / 2

    try:
        subprocess.check_output(
            [
                os.environ.get(
                    'PATH_FFMPEG',
                    '/usr/local/bin/ffmpeg'
                ),
                '-v',
                'fatal',
                '-ss',
                str(midpoint),
                '-i',
                video_path,
                '-vframes',
                '1',  # output 1 frame
                thumb_path
            ],
            stderr=subprocess.STDOUT
        )
        print("Used ffmpeg to convert {video_path} to {thumb_path}")
        return True
    except subprocess.CalledProcessError as e:
        print(
            f"ERROR: ffmpeg thumbnail creation failed: {e.cmd}\n"
            f"returncode was: {e.returncode}\n"
            f"output was: {e.output}"
        )
        return False


def make_thumbnail(source_file_path, mimetype):
    '''
        generate thumbnail image for PDF, video, or image
    '''
    thumbnail_path = None
    success = False

    if mimetype not in ['image/jpeg', 'application/pdf', 'video/mp4']:
        raise UnsupportedMimetype(f"thumbnail: {mimetype}")

    if mimetype == 'image/jpeg':
        success = True
        thumbnail_path = source_file_path

    thumb_name = f"{source_file_path.split('.')[0]}.png"
    thumbnail_path = os.path.join('/tmp', thumb_name)

    if mimetype == 'application/pdf':
        success = make_thumb_from_pdf(
            f"{source_file_path}[0]", thumbnail_path)

    if mimetype == 'video/mp4':
        success = make_thumb_from_video(
            source_file_path, thumbnail_path)

    if not success:
        raise ThumbnailError(
            f"ERROR: creating a thumbnail: "
            f"{source_file_path}, mimetype: {mimetype}"
        )
    return thumbnail_path


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


def tiff_conversion(input_path):
    '''
        convert file using ImageMagick `convert`:
        http://www.imagemagick.org/script/convert.php
    '''
    magick_location = os.environ.get(
        'PATH_MAGICK_CONVERT', '/usr/local/bin/convert')
    output_path = f"{input_path.split('.')[0]}.tif"
    proc = [
        magick_location, "-compress", "None",
        "-quality", "100", "-auto-orient", input_path, output_path
    ]
    msg = (
        f"Used ImagMagick convert to convert {input_path} "
        f"to {output_path}"
    )
    try:
        subprocess.check_output(proc, stderr=subprocess.STDOUT)
        print(msg)
    except subprocess.CalledProcessError as e:
        print(
            f"ERROR: ImageMagick `convert` failed: {e.cmd}\n"
            f"returncode was: {e.returncode}\n"
            f"output was: {e.output}"
        )
        return False


def tiff_to_srgb_libtiff(input_path):
    '''
    convert color profile to sRGB using libtiff's `tiff2rgba` tool
    '''
    tiff2rgba_location = os.environ.get(
        'PATH_TIFF2RGBA', '/usr/local/bin/tiff2rgba')
    output_path = f"{input_path.split('.')[0]}.tiff"
    proc = [tiff2rgba_location, "-c", "none", input_path, output_path]
    msg = (
        f"Used tiff2rgba to convert {input_path} to {output_path}, "
        "with color profile sRGB (if not already sRGB)"
    )
    try:
        subprocess.check_output(proc, stderr=subprocess.STDOUT)
        print(msg)
    except subprocess.CalledProcessError as e:
        print(
            f"ERROR: `tiff2rgba` failed: {e.cmd}\n"
            f"returncode was: {e.returncode}\n"
            f"output was: {e.output}"
        )
        return False


def uncompress_jp2000(compressed_path):
    ''' uncompress a jp2000 file using kdu_expand '''
    kdu_expand_location = os.environ.get(
        'PATH_KDU_EXPAND', '/usr/local/bin/kdu_expand')
    output_path = f"{compressed_path.split('.')[0]}.tiff"
    proc = [
        kdu_expand_location, "-i", compressed_path, "-o", output_path
    ]
    msg = (
        f"File uncompressed using kdu_expand. Input: "
        f"{compressed_path}, output: {output_path}"
    )
    try:
        subprocess.check_output(proc, stderr=subprocess.STDOUT)
        print(msg)
    except subprocess.CalledProcessError as e:
        print(
            f"ERROR: `kdu_expand` failed: {e.cmd}\n"
            f"returncode was: {e.returncode}\n"
            f"output was: {e.output}"
        )
        return False


def tiff_to_jp2(tiff_path):
    ''' convert a tiff to jp2 using kdu_compress.
    tiff must be uncompressed.'''
    kdu_compress_location = os.environ.get(
        'PATH_KDU_COMPRESS', '/usr/local/bin/kdu_compress')
    jp2_path = f"{tiff_path.split('.')[0]}.jp2"
    proc = [kdu_compress_location, "-i", tiff_path, "-o", jp2_path]
    proc.extend(KDU_COMPRESS_DEFAULT_OPTS)
    msg = "{tiff_path} converted to {jp2_path}"

    try:
        subprocess.check_output(proc, stderr=subprocess.STDOUT)
        print(msg)
    except subprocess.CalledProcessError as e:
        print(f"A kdu_compress command failed. Trying alternate:\n{e}")
        proc = [kdu_compress_location, "-i", tiff_path, "-o", jp2_path]
        proc.extend(KDU_COMPRESS_BASE_OPTS)
        try:
            subprocess.check_output(proc, stderr=subprocess.STDOUT)
            print(msg)
        except subprocess.CalledProcessError as e:
            print(
                f"ERROR: `kdu_compress` failed: {e.cmd}\n"
                f"returncode was: {e.returncode}\n"
                f"output was: {e.output}"
            )

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
    elif mimetype in ['image/jp2', 'image/jpx', 'image/jpm']:
        prepped_file_path = uncompress_jp2000(source_file_path)

    if not prepped_file_path:
        print(
            "Didn't know how to prep file with mimetype {mimetype} for "
            "jp2 conversion"
        )
        return

    jp2_filepath = tiff_to_jp2(prepped_file_path)
    if not jp2_filepath:
        shutil.rmtree(tmp_dir)
        return

    return jp2_filepath
