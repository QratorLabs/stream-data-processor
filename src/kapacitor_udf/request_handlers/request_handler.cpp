#include "request_handler.h"

RequestHandler::RequestHandler(const std::shared_ptr<IUDFAgent> &agent)
    : agent_(agent) {

}
