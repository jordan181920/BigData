case class Event(actor: Actor,
                 created_at: String,
                 id: String,
                 org: String,
                 payload: Payload,
                 public: Boolean,
                 repo: String,
                 `type`: String)