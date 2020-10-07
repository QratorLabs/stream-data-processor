ARG GCC_IMAGE_VERSION=latest

FROM gcc:${GCC_IMAGE_VERSION}

ENV CPPZMQ_VERSION=4.6.0
ENV CATCH2_VERSION=2.13.1

ENV ARROW_DEB_PCKG_NAME="apache-arrow.deb"
ENV CPPZMQ_DIR_NAME="cppzmq-${CPPZMQ_VERSION}"
ENV CATCH2_DIR_NAME="Catch2-${CATCH2_VERSION}"

ENV ENV_ARROW_DEB_PCKG_SHA256="fdc8c22d411e62bcaa7bf9e2fd2d252ef40157a166c9acd7cea08670453383ab"
ENV ENV_CPPZMQ_SHA256="e9203391a0b913576153a2ad22a2dc1479b1ec325beb6c46a3237c669aef5a52"
ENV ENV_CATCH2_SHA256="36bcc9e6190923961be11e589d747e606515de95f10779e29853cfeae560bd6c"

#ENV PROJECT_PATH="/stream_data_processor"
#
#COPY . "${PROJECT_PATH}"

SHELL ["/bin/bash", "-c"]

# Configure system for further build and run
RUN apt-get -o Acquire::Check-Valid-Until=false update \
    && apt-get install --no-install-recommends --no-install-suggests --yes --verbose-versions autoconf automake libtool curl g++ unzip cmake ninja-build wget tar pkg-config ca-certificates lsb-release git libzmq3-dev libspdlog-dev libprotobuf-dev protobuf-compiler \
    \
    && if [ "${ENV_ARROW_DEB_PCKG_SHA256}" = "" ]; then echo "arrow deb package sha256 hash sum environment variable is empty. Exiting..." ; exit 1 ; fi \
    && wget -O "${ARROW_DEB_PCKG_NAME}" "https://apache.bintray.com/arrow/debian/apache-arrow-archive-keyring-latest-$(lsb_release --codename --short).deb" \
    && echo "${ENV_ARROW_DEB_PCKG_SHA256} ${ARROW_DEB_PCKG_NAME}" | sha256sum -c \
    && apt-get install --no-install-recommends --no-install-suggests --yes --verbose-versions  "./${ARROW_DEB_PCKG_NAME}" \
    && apt-get -o Acquire::Check-Valid-Until=false update \
    && apt-get install --no-install-recommends --no-install-suggests --yes --verbose-versions libarrow-dev libgandiva-dev \
    \
    && if [ "${ENV_CPPZMQ_SHA256}" = "" ]; then echo "cppzmq sha256 hash sum environment variable is empty. Exiting..." ; exit 1 ; fi \
    && wget -O "${CPPZMQ_DIR_NAME}.tar.gz" "https://github.com/zeromq/cppzmq/archive/v${CPPZMQ_VERSION}.tar.gz" \
    && echo "${ENV_CPPZMQ_SHA256} ${CPPZMQ_DIR_NAME}.tar.gz" | sha256sum -c \
    && tar -xvzf "${CPPZMQ_DIR_NAME}.tar.gz" \
    && mkdir "${CPPZMQ_DIR_NAME}/build" && pushd "${CPPZMQ_DIR_NAME}/build" && cmake .. -G Ninja && ninja -l0 -v -d stats install && popd \
    \
    && if [ "${ENV_CATCH2_SHA256}" = "" ]; then echo "Catch2 sha256 hash sum environment variable is empty. Exiting..." ; exit 1 ; fi \
    && wget -O "${CATCH2_DIR_NAME}.tar.gz" "https://github.com/catchorg/Catch2/archive/v${CATCH2_VERSION}.tar.gz" \
    && echo "${ENV_CATCH2_SHA256} ${CATCH2_DIR_NAME}.tar.gz" | sha256sum -c \
    && tar -xvzf "${CATCH2_DIR_NAME}.tar.gz" \
    && pushd "${CATCH2_DIR_NAME}" && cmake -B"build" -H. -D"BUILD_TESTING"=OFF -G Ninja && cmake --build build/ --target install && popd

#CMD bash "${PROJECT_PATH}/test_script.sh"
