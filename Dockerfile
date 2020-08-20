FROM gcc:10.2.0

ENV CPPZMQ_VERSION=4.6.0

RUN apt-get update \
    && apt-get install -y -V cmake make wget tar pkg-config ca-certificates lsb-release git libzmq3-dev \
    && if [ "${ARROW_DEB_PCKG_SHA256}" = "" ]; then echo "arrow deb package sha256 hash sum environment variable is empty. Exiting..." ; exit 1 ; fi \
    && wget https://apache.bintray.com/arrow/debian/apache-arrow-archive-keyring-latest-buster.deb \
    && if [ "$(sha256sum apache-arrow-archive-keyring-latest-buster.deb)" != "${ARROW_DEB_PCKG_SHA256}" ]; then echo "Bad SHA256 hash sum of apache-arrow-archive-keyring-latest-buster.deb" ; exit 1 ; fi \
    && apt-get install -y -V ./apache-arrow-archive-keyring-latest-$(lsb_release --codename --short).deb \
    && apt-get update \
    && apt-get install -y -V libarrow-dev libgandiva-dev \
    && git clone https://github.com/gabime/spdlog.git \
    && cd spdlog && mkdir build && cd build \
    && cmake .. && make -j install && cd ../.. \
    && if [ "${CPPZMQ_VERSION}" = "" ]; then echo "cppzmq sha256 hash sum environment variable is empty. Exiting..." ; exit 1 ; fi \
    && wget https://github.com/zeromq/cppzmq/archive/v${CPPZMQ_VERSION}.tar.gz \
    && if [ "$(sha256sum v${CPPZMQ_VERSION}.tar.gz)" != "${CPPZMQ_SHA256}" ]; then echo "Bad SHA256 hash sum of v${CPPZMQ_VERSION}.tar.gz" ; exit 1 ; fi \
    && tar -xvzf v${CPPZMQ_VERSION}.tar.gz && cd cppzmq-${CPPZMQ_VERSION} && mkdir build && cd build && cmake .. && make -j4 install && cd ../.. \
    && git clone https://github.com/catchorg/Catch2.git \
    && cd Catch2 \
    && cmake -Bbuild -H. -DBUILD_TESTING=OFF \
    && cmake --build build/ --target install
