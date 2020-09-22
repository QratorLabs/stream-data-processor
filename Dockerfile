ARG GCC_IMAGE_VERSION=latest

FROM gcc:$GCC_IMAGE_VERSION

ENV CPPZMQ_VERSION=4.6.0
ENV PROTOBUF_VERSION=3.12.3
ENV CATCH2_VERSION=2.13.1

ENV CPPZMQ_DIR_NAME="cppzmq-${CPPZMQ_VERSION}"
ENV PROTOBUF_DIR_NAME="protobuf-${PROTOBUF_VERSION}"
ENV CATCH2_DIR_NAME="Catch2-${CATCH2_VERSION}"

ARG ARROW_DEB_PCKG_SHA256
ENV ENV_ARROW_DEB_PCKG_SHA256=$ARROW_DEB_PCKG_SHA256

ARG CPPZMQ_SHA256
ENV ENV_CPPZMQ_SHA256=$CPPZMQ_SHA256

ARG PROTOBUF_SHA256
ENV ENV_PROTOBUF_SHA256=$PROTOBUF_SHA256

ARG CATCH2_SHA256
ENV ENV_CATCH2_SHA256=$CATCH2_SHA256

# Configure system for further build and run
RUN apt-get update \
    && apt-get install -y -V autoconf automake libtool curl g++ unzip cmake make wget tar pkg-config ca-certificates lsb-release git libzmq3-dev \
    \
    && if [ "${ENV_ARROW_DEB_PCKG_SHA256}" = "" ]; then echo "arrow deb package sha256 hash sum environment variable is empty. Exiting..." ; exit 1 ; fi \
    && wget "https://apache.bintray.com/arrow/debian/apache-arrow-archive-keyring-latest-$(lsb_release --codename --short).deb" \
    && if [ "$(sha256sum apache-arrow-archive-keyring-latest-$(lsb_release --codename --short).deb)" != "${ENV_ARROW_DEB_PCKG_SHA256}" ]; then echo "Bad SHA256 hash sum of apache-arrow-archive-keyring-latest-buster.deb" ; exit 1 ; fi \
    && apt-get install -y -V ./apache-arrow-archive-keyring-latest-$(lsb_release --codename --short).deb \
    && apt-get update \
    && apt-get install -y -V libarrow-dev libgandiva-dev \
    \
    && if [ "${ENV_CPPZMQ_SHA256}" = "" ]; then echo "cppzmq sha256 hash sum environment variable is empty. Exiting..." ; exit 1 ; fi \
    && wget -O "${CPPZMQ_DIR_NAME}.tar.gz" "https://github.com/zeromq/cppzmq/archive/v${CPPZMQ_VERSION}.tar.gz" \
    && if [ "$(sha256sum ${CPPZMQ_DIR_NAME}.tar.gz)" != "${ENV_CPPZMQ_SHA256}" ]; then echo "Bad SHA256 hash sum of ${CPPZMQ_DIR_NAME}.tar.gz" ; exit 1 ; fi \
    && tar -xvzf "${CPPZMQ_DIR_NAME}.tar.gz" \
    && mkdir "${CPPZMQ_DIR_NAME}/build" && pushd "${CPPZMQ_DIR_NAME}/build" && cmake .. && make -j4 install && popd \
    \
    && if [ "${ENV_CATCH2_SHA256}" = "" ]; then echo "Catch2 sha256 hash sum environment variable is empty. Exiting..." ; exit 1 ; fi \
    && wget -O "${CATCH2_DIR_NAME}.tar.gz" "https://github.com/catchorg/Catch2/archive/v${CATCH2_VERSION}.tar.gz" \
    && if [ "$(sha256sum ${CATCH2_DIR_NAME}.tar.gz)" != "${ENV_CATCH2_SHA256}" ]; then echo "Bad SHA256 hash sum of ${CATCH2_DIR_NAME}.tar.gz" ; exit 1 ; fi \
    && tar -xvzf "${CATCH2_DIR_NAME}" \
    && pushd "${CATCH2_DIR_NAME}" && cmake -Bbuild -H. -DBUILD_TESTING=OFF && cmake --build build/ --target install && popd \
    \
    && if [ "${ENV_PROTOBUF_SHA256}" = "" ]; then echo "protobuf sha256 hash sum environment variable is empty. Exiting..." ; exit 1 ; fi \
    && wget -O "${PROTOBUF_DIR_NAME}.tar.gz" "https://github.com/protocolbuffers/protobuf/releases/download/v${PROTOBUF_VERSION}/protobuf-cpp-${PROTOBUF_VERSION}.tar.gz" \
    && if [ "$(sha256sum ${PROTOBUF_DIR_NAME}.tar.gz)" != "${ENV_PROTOBUF_SHA256}" ]; then echo "Bad SHA256 hash sum of ${PROTOBUF_DIR_NAME}.tar.gz" ; exit 1 ; fi \
    && tar --no-same-owner -xzvf "${PROTOBUF_DIR_NAME}.tar.gz" \
    && pushd "${PROTOBUF_DIR_NAME}" && ./configure && make install && ldconfig
