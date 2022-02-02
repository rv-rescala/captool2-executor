FROM public.ecr.aws/lambda/python@sha256:2342562ecf32e72dfad88f8267ad39d78f1d397415d47a4a208ac243f067dd67 as build

RUN mkdir /opt/browser 
RUN mkdir /opt/browser/.fonts

RUN yum install -y unzip && \
    curl -Lo "/tmp/chromedriver.zip" "https://chromedriver.storage.googleapis.com/97.0.4692.71/chromedriver_linux64.zip" && \
    curl -Lo "/tmp/chrome-linux.zip" "https://www.googleapis.com/download/storage/v1/b/chromium-browser-snapshots/o/Linux_x64%2F938549%2Fchrome-linux.zip?alt=media" && \
    unzip /tmp/chromedriver.zip -d /opt/browser/ && \
    unzip /tmp/chrome-linux.zip -d /opt/browser/

RUN yum install atk cups-libs gtk3 libXcomposite alsa-lib \
    libXcursor libXdamage libXext libXi libXrandr libXScrnSaver \
    libXtst pango at-spi2-atk libXt xorg-x11-server-Xvfb \
    xorg-x11-xauth dbus-glib dbus-glib-devel -y
RUN pip install captool2

RUN yum update && yum install -y wget unzip
RUN wget https://moji.or.jp/wp-content/ipafont/IPAexfont/IPAexfont00401.zip \
    && unzip IPAexfont00401.zip -d /opt/browser/.fonts/ \
    && rm IPAexfont00401.zip

# for jappanese
RUN yum install -y ipa-gothic-fonts ipa-mincho-fonts ipa-pgothic-fonts ipa-pmincho-fonts

COPY lambda.py ./
CMD [ "lambda.handler" ]
