#ifndef CONTEXT_H
#define CONTEXT_H

class Context {
  Context(const Context& other);
  const Context& operator=(const Context& other);

 protected:
  virtual void finish(int r) = 0;

 public:
  Context() {}
  virtual ~Context() {}       // we want a virtual destructor!!!
  virtual void complete(int r) {
    finish(r);
    delete this;
  }
  virtual void complete_without_del(int r) {
    finish(r);
  }
  int ret;
  int queue_id;
};

#endif //CONTEXT_H