ARG ALPINE_IMAGE_VERSION=latest

FROM alpine:${ALPINE_IMAGE_VERSION} AS arrow-base
ENV ARROW_VERSION=3.0.0
ENV ARROW_DIR_NAME="arrow-apache-arrow-${ARROW_VERSION}"
ENV ENV_ARROW_SHA256="fc461c4f0a60e7470a7c58b28e9344aa8fb0be5cc982e9658970217e084c3a82"
RUN apk add --no-cache wget tar autoconf bash cmake g++ gcc make protobuf-dev clang llvm-static llvm-dev python3 re2-dev boost-dev
SHELL ["bash", "-c"]
RUN if [ "${ENV_ARROW_SHA256}" = "" ]; then echo "Arrow sha256 hash sum environment variable is empty. Exiting..." ; exit 1 ; fi \
    && wget -O "${ARROW_DIR_NAME}.tar.gz" "https://github.com/apache/arrow/archive/apache-arrow-${ARROW_VERSION}.tar.gz" \
    && echo "${ENV_ARROW_SHA256}  ${ARROW_DIR_NAME}.tar.gz" | sha256sum -c \
    && tar -xvzf "${ARROW_DIR_NAME}.tar.gz" \
    && mkdir "${ARROW_DIR_NAME}/cpp/release" && pushd "${ARROW_DIR_NAME}/cpp/release" \
    && cmake .. -DARROW_COMPUTE=ON -DARROW_CSV=ON -DARROW_IPC=ON -DARROW_GANDIVA=ON && make install

FROM arrow-base AS system-config
RUN apk add --no-cache ninja spdlog-dev git zeromq-dev catch2 clang-extra-tools
ENV CPPZMQ_VERSION=4.6.0
ENV CPPZMQ_DIR_NAME="cppzmq-${CPPZMQ_VERSION}"
ENV ENV_CPPZMQ_SHA256="e9203391a0b913576153a2ad22a2dc1479b1ec325beb6c46a3237c669aef5a52"
RUN if [ "${ENV_CPPZMQ_SHA256}" = "" ]; then echo "cppzmq sha256 hash sum environment variable is empty. Exiting..." ; exit 1 ; fi \
    && wget -O "${CPPZMQ_DIR_NAME}.tar.gz" "https://github.com/zeromq/cppzmq/archive/v${CPPZMQ_VERSION}.tar.gz" \
    && echo "${ENV_CPPZMQ_SHA256}  ${CPPZMQ_DIR_NAME}.tar.gz" | sha256sum -c \
    && tar -xvzf "${CPPZMQ_DIR_NAME}.tar.gz" \
    && mkdir "${CPPZMQ_DIR_NAME}/build" && pushd "${CPPZMQ_DIR_NAME}/build" && cmake .. -D"CPPZMQ_BUILD_TESTS"=OFF -G Ninja && ninja -v install && popd

FROM system-config AS builder
ARG CMAKE_BUILD_BINARY_TARGET="test_main"
ARG PROJECT_ID="sdp"
LABEL stage="builder"
LABEL project="${PROJECT_ID}"
COPY . /stream-data-processor
WORKDIR /stream-data-processor/build/
RUN if [ "${CMAKE_BUILD_BINARY_TARGET}" = "test_main" ]; then cmake .. -DENABLE_TESTS=ON -DCLANG_TIDY_LINT=OFF -DBUILD_SHARED_LIBS=OFF ; else cmake .. -DENABLE_TESTS=OFF -DCLANG_TIDY_LINT=OFF -DBUILD_SHARED_LIBS=OFF ; fi \
    && make "${CMAKE_BUILD_BINARY_TARGET}" -j$(( $(nproc) / 2 + 1 ))

FROM alpine:${ALPINE_IMAGE_VERSION} AS app
ARG CMAKE_BUILD_BINARY_TARGET
ARG PROJECT_ID
LABEL stage="app"
LABEL project="${PROJECT_ID}"
COPY --from=builder "/stream-data-processor/build/bin/${CMAKE_BUILD_BINARY_TARGET}" ./app/
RUN apk add --no-cache bash libstdc++ spdlog libprotobuf zeromq catch2 re2
