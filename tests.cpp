#include "cppq.hpp"

#undef NDEBUG

const std::string TypeEmailDelivery = "email:deliver";

typedef struct {
  int UserID;
  std::string TemplateID;
} EmailDeliveryPayload;

void to_json(nlohmann::json& j, const EmailDeliveryPayload& p) {
  j = nlohmann::json{{"UserID", p.UserID}, {"TemplateID", p.TemplateID}};
}

cppq::Task NewEmailDeliveryTask(EmailDeliveryPayload payload) {
  nlohmann::json j = payload;
  return cppq::Task{TypeEmailDelivery, j, 10};
}

void HandleEmailDeliveryTask(cppq::Task& task) {
  int userID = task.payload["UserID"];
  std::string templateID = task.payload["TemplateID"];

  nlohmann::json r;
  r["Sent"] = true;
  task.result = r;
  return;
}

int main(int argc, char *argv[]) {
  cppq::registerHandler(TypeEmailDelivery, &HandleEmailDeliveryTask);

  redisOptions options = {0};
  REDIS_OPTIONS_SET_TCP(&options, "127.0.0.1", 6379);
  redisContext *c = redisConnectWithOptions(&options);
  if (c == NULL || c->err) {
    std::cerr << "Failed to connect to Redis" << std::endl;
    return 1;
  }

  cppq::Task task = NewEmailDeliveryTask(EmailDeliveryPayload{.UserID = 666, .TemplateID = "AH"});

  cppq::enqueue(c, task);

  redisReply *reply = (redisReply *)redisCommand(c, "LRANGE cppq:pending -1 -1");
  if (reply->type != REDIS_REPLY_ARRAY)
    assert(false);
  if (reply->elements == 0)
    assert(false);
  reply = reply->element[0];
  std::string uuid = reply->str;

  redisCommand(c, "MULTI");
  redisCommand(c, "LREM cppq:pending 1 %s", uuid.c_str());
  redisCommand(c, "HGET cppq:task:%s type", uuid.c_str());
  redisCommand(c, "HGET cppq:task:%s payload", uuid.c_str());
  redisCommand(c, "HGET cppq:task:%s state", uuid.c_str());
  redisCommand(c, "HGET cppq:task:%s maxRetry", uuid.c_str());
  redisCommand(c, "HGET cppq:task:%s retried", uuid.c_str());
  redisCommand(c, "HGET cppq:task:%s dequeuedAtMs", uuid.c_str());
  reply = (redisReply *)redisCommand(c, "EXEC");

  if (reply->type != REDIS_REPLY_ARRAY || reply->elements != 7)
    assert(false);

  task = cppq::Task(
    uuid,
    reply->element[1]->str,
    reply->element[2]->str,
    reply->element[3]->str,
    strtoull(reply->element[4]->str, NULL, 0),
    strtoull(reply->element[5]->str, NULL, 0),
    strtoull(reply->element[6]->str, NULL, 0)
  );

  assert(task.type.compare(TypeEmailDelivery) == 0);
  assert(task.payload["UserID"] == 666);
  assert(task.payload["TemplateID"].get<std::string>().compare("AH") == 0);
  assert(task.state == cppq::TaskState::Pending);
  assert(task.maxRetry == 10);
  assert(task.retried == 0);
  assert(task.dequeuedAtMs == 0);
}
