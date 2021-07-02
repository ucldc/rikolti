FROM python:3

RUN grep '^deb ' /etc/apt/sources.list | perl -pe 's/deb /deb-src /' >> /etc/apt/sources.list

RUN apt-get update -qq && apt-get install -y python3-lxml pylint libxml2-dev libxslt1-dev python3-dev zlib1g-dev lib32z1-dev libffi-dev libssl-dev libjpeg-dev ghostscript imagemagick libopenjp2-7-dev libtiff-dev && apt-get build-dep -y lxml

RUN sed -i 's/<policy domain="coder" rights="none" pattern="PDF" \/>/<!--<policy domain="coder" rights="none" pattern="PDF" \/>-->/' /etc/ImageMagick-6/policy.xml

WORKDIR /app

# Add file-fetcher
COPY / /app

RUN cd /app && pip install --upgrade pip && pip install Cython && pip install -r requirements.txt


ENTRYPOINT ["python", "fetch_collection_files.py"]






