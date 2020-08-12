#include "producer.h"

Producer::Producer(std::shared_ptr<Node> node) : node_(std::move(node)) {

}