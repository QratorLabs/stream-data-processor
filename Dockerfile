ARG GCC_IMAGE_VERSION=latest

FROM gcc:$GCC_IMAGE_VERSION

ENV CPPZMQ_VERSION=4.6.0
ENV CATCH2_VERSION=2.13.1

ENV ARROW_DEB_PCKG_NAME="apache-arrow.deb"
ENV CPPZMQ_DIR_NAME="cppzmq-${CPPZMQ_VERSION}"
ENV CATCH2_DIR_NAME="Catch2-${CATCH2_VERSION}"

ENV ENV_ARROW_DEB_PCKG_SHA256="fdc8c22d411e62bcaa7bf9e2fd2d252ef40157a166c9acd7cea08670453383ab  ${ARROW_DEB_PCKG_NAME}"
ENV ENV_CPPZMQ_SHA256="e9203391a0b913576153a2ad22a2dc1479b1ec325beb6c46a3237c669aef5a52  ${CPPZMQ_DIR_NAME}.tar.gz"
ENV ENV_CATCH2_SHA256="36bcc9e6190923961be11e589d747e606515de95f10779e29853cfeae560bd6c  ${CATCH2_DIR_NAME}.tar.gz"

# Configure system for further build and run
RUN apt-get update \
    && apt-get install -y -V autoconf automake libtool curl g++ unzip cmake make wget tar pkg-config ca-certificates lsb-release git libzmq3-dev libprotobuf-dev \
    \
    && if [ "${ENV_ARROW_DEB_PCKG_SHA256}" = "" ]; then echo "arrow deb package sha256 hash sum environment variable is empty. Exiting..." ; exit 1 ; fi \
    && wget -O "${ARROW_DEB_PCKG_NAME}" "https://apache.bintray.com/arrow/debian/apache-arrow-archive-keyring-latest-$(lsb_release --codename --short).deb" \
    && if [ "$(sha256sum ${ARROW_DEB_PCKG_NAME})" != "${ENV_ARROW_DEB_PCKG_SHA256}" ]; then echo "Bad SHA256 hash sum of ${ARROW_DEB_PCKG_NAME}" ; exit 1 ; fi \
    && apt-get install -y -V ./${ARROW_DEB_PCKG_NAME} \
    && apt-get update \
    && apt-get install -y -V libarrow-dev libgandiva-dev \
    \
    && if [ "${ENV_CPPZMQ_SHA256}" = "" ]; then echo "cppzmq sha256 hash sum environment variable is empty. Exiting..." ; exit 1 ; fi \
    && wget -O "${CPPZMQ_DIR_NAME}.tar.gz" "https://github.com/zeromq/cppzmq/archive/v${CPPZMQ_VERSION}.tar.gz" \
    && if [ "$(sha256sum ${CPPZMQ_DIR_NAME}.tar.gz)" != "${ENV_CPPZMQ_SHA256}" ]; then echo "Bad SHA256 hash sum of ${CPPZMQ_DIR_NAME}.tar.gz" ; exit 1 ; fi \
    && tar -xvzf "${CPPZMQ_DIR_NAME}.tar.gz" \
    && mkdir "${CPPZMQ_DIR_NAME}/build" && cd "${CPPZMQ_DIR_NAME}/build" && cmake .. && make -j4 install && cd ../.. \
    \
    && if [ "${ENV_CATCH2_SHA256}" = "" ]; then echo "Catch2 sha256 hash sum environment variable is empty. Exiting..." ; exit 1 ; fi \
    && wget -O "${CATCH2_DIR_NAME}.tar.gz" "https://github.com/catchorg/Catch2/archive/v${CATCH2_VERSION}.tar.gz" \
    && if [ "$(sha256sum ${CATCH2_DIR_NAME}.tar.gz)" != "${ENV_CATCH2_SHA256}" ]; then echo "Bad SHA256 hash sum of ${CATCH2_DIR_NAME}.tar.gz" ; exit 1 ; fi \
    && tar -xvzf "${CATCH2_DIR_NAME}.tar.gz" \
    && cd "${CATCH2_DIR_NAME}" && cmake -Bbuild -H. -DBUILD_TESTING=OFF && cmake --build build/ --target install && cd ..
