use j4rs::{InvocationArg, JvmBuilder, MavenArtifact, errors::Result, Jvm, Instance, Null};
use j4rs::errors::J4RsError;

struct TextMessage<'a> {
    jvm: &'a Jvm,
    instance: Instance,
}

impl<'a> TextMessage<'a> {

    fn new(jvm: &'a Jvm, instance: Instance) -> Self {
        Self { jvm, instance }
    }

    fn get_text(&self) -> Result<String> {
        let text_instance = self.jvm.invoke(
            &self.instance,
            "getText",
            InvocationArg::empty(),
        )?;
        let text: String = self.jvm.to_rust(text_instance)?;
        Ok(text)
    }

    fn get_jms_message_id(&self) -> Result<String> {
        let id_instance = self.jvm.invoke(
            &self.instance,
            "getJMSMessageID",
            InvocationArg::empty(),
        )?;
        let id: String = self.jvm.to_rust(id_instance)?;
        Ok(id)
    }

}

enum Message<'a> {
    TextMessage(TextMessage<'a>),
}

impl<'a> Message<'a> {
    fn new(jvm: &'a Jvm, instance: Instance) -> Result<Self> {
        let class: String = jvm
            .chain(&instance)?
            .cast("java.lang.Object")?
            .invoke("getClass", InvocationArg::empty())?
            .invoke("getName", InvocationArg::empty())?
            .to_rust()?;

        if class.contains("TextMessage") {
            let text_instance = jvm.cast(&instance, "jakarta.jms.TextMessage")?;
            Ok(Self::TextMessage(TextMessage::new(jvm, text_instance)))
        } else {
            Err(J4RsError::GeneralError(String::from("This Message Type is currently not supported.")))
        }

    }

    fn get_jms_message_id(&self) -> Result<String> {
        match self {
            Message::TextMessage(text) => text.get_jms_message_id()
        }
    }

}
impl From<Message<'_>> for InvocationArg {
    fn from(value: Message<'_>) -> Self {
        let instance = match value {
            Message::TextMessage(text) => text.instance
        };
        InvocationArg::from(instance)
    }
}

struct Queue {
    instance: Instance,
}

impl Queue {
    fn new(instance: Instance) -> Self {
        Queue { instance }
    }

}

struct Consumer<'a> {
    jvm: &'a Jvm,
    instance: Instance
}

impl<'a> Consumer<'a> {

    fn new(jvm: &'a Jvm, instance: Instance) -> Self {
        Consumer { jvm, instance }
    }

    fn receive_no_wait(&'_ self) -> Result<Option<Message<'_>>> {
        let message = self.jvm.invoke(
            &self.instance,
            "receiveNoWait",
            InvocationArg::empty(),
        )?;
        let is_null = self.jvm.check_equals(&message, InvocationArg::try_from(Null::Boolean)?)?;
        if is_null {
            Ok(None)
        } else {
            Ok(Some(Message::new(self.jvm, message)?))
        }
    }
}

struct Producer<'a> {
    jvm: &'a Jvm,
    instance: Instance,
}

impl<'a> Producer<'a> {
    fn new(jvm: &'a Jvm, instance: Instance) -> Self {
        Self { jvm, instance }
    }

    fn send(&self, message: Message) -> Result<()> {
        self.jvm.invoke(
            &self.instance,
            "send",
            &[InvocationArg::from(message)],
        )?;
        Ok(())
    }
}

struct Session<'a> {
    jvm: &'a Jvm,
    instance: Instance,
}

impl<'a> Session<'a> {

    fn new(jvm: &'a Jvm, instance: Instance) -> Self {
        Self { jvm, instance }
    }

    fn create_text_message(&self, text: &str) -> Result<TextMessage<'a>> {
        let message = self.jvm.invoke(
            &self.instance,
            "createTextMessage",
            &[InvocationArg::try_from(text)?],
        )?;
        Ok(TextMessage::new(self.jvm, message))
    }

    fn create_queue(&self, name: &str) -> Result<Queue> {
        let queue = self.jvm.invoke(
            &self.instance,
            "createQueue",
            &[InvocationArg::try_from(name)?],
        )?;
        Ok(Queue::new(queue))
    }

    fn create_producer(&'_ self, queue: Queue) -> Result<Producer<'_>> {
        let producer = self.jvm.invoke(
            &self.instance,
            "createProducer",
            &[InvocationArg::from(queue.instance)],
        )?;
        Ok(Producer::new(self.jvm, producer))
    }

    fn create_consumer(&'_ self, queue: Queue) -> Result<Consumer<'_>> {
        let consumer = self.jvm.invoke(
            &self.instance,
            "createConsumer",
            &[InvocationArg::from(queue.instance)],
        )?;
        Ok(Consumer::new(self.jvm, consumer))
    }

    fn close(&mut self) -> Result<()> {
        self.jvm.invoke(
            &self.instance,
            "close",
            InvocationArg::empty(),
        )?;
        Ok(())
    }
}

impl Drop for Session<'_> {
    fn drop(&mut self) {
        self.close().ok();
    }
}

struct Connection<'a> {
    jvm: &'a Jvm,
    instance: Instance,
}

impl<'a> Connection<'a> {
    fn new(jvm: &'a Jvm, instance: Instance) -> Connection<'a> {
        Self { jvm, instance }
    }

    fn start(&self) -> Result<()> {
        self.jvm.invoke(
            &self.instance,
            "start",
            InvocationArg::empty(),
        )?;
        Ok(())
    }

    fn create_session(&'_ self) -> Result<Session<'_>> {
        let session = self.jvm.invoke(
            &self.instance,
            "createSession",
            InvocationArg::empty(),
        )?;
        Ok(Session::new(self.jvm, session))
    }

    fn close(&mut self) -> Result<()> {
        self.jvm.invoke(
            &self.instance,
            "close",
            InvocationArg::empty(),
        )?;
        Ok(())
    }
}

impl Drop for Connection<'_> {
    fn drop(&mut self) {
        self.close().ok();
    }
}

struct ConnectionFactory<'a> {
    jvm: &'a Jvm,
    instance: Instance,
}

impl<'a> ConnectionFactory<'a> {

    fn new(jvm: &'a Jvm, factory_name: &str) -> Result<ConnectionFactory<'a>> {
        let instance = jvm.create_instance(
            factory_name,
            InvocationArg::empty()
        )?;
        Ok(ConnectionFactory { jvm, instance })
    }

    fn create_connection(&self, username: &str, password: &str) -> Result<Connection<'a>> {
        let instance = self.jvm.invoke(
            &self.instance,
            "createConnection",
            &[
                InvocationArg::try_from(username)?,
                InvocationArg::try_from(password)?,
            ],
        )?;
        Ok(Connection::new(self.jvm, instance))
    }

    fn close(&mut self) -> Result<()> {
        self.jvm.invoke(
            &self.instance,
            "close",
            InvocationArg::empty(),
        )?;
        Ok(())
    }

}

impl Drop for ConnectionFactory<'_> {
    fn drop(&mut self) {
        self.close().ok();
    }
}

fn main() -> Result<()> {
    let jvm = JvmBuilder::new().build()?;

    println!("Loading the jms driver. This may take some time.");
    let dbx_artifact = MavenArtifact::from("org.apache.artemis:artemis-jakarta-client:2.53.0");
    jvm.deploy_artifact_and_deps(&dbx_artifact)?;
    println!("Loaded the jms driver");

    let connection_factory = ConnectionFactory::new
        (&jvm, "org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory")?;

    let connection = connection_factory.create_connection("artemis", "artemis")?;

    connection.start()?;

    let session = connection.create_session()?;

    let message = session.create_text_message("Hello from Rust")?;

    let queue = session.create_queue("TEST::TEST_ANYCAST")?;

    let producer = session.create_producer(queue)?;

    producer.send(Message::TextMessage(message))?;

    let queue = session.create_queue("TEST_ANYCAST")?;

    let consumer = session.create_consumer(queue)?;

    let message = consumer.receive_no_wait()?;

    if let Some(msg) = message {
        println!("Message-Id: {:?}", msg.get_jms_message_id()?);
        let text = match msg {
            Message::TextMessage(message) => {message.get_text()}
        }?;
        println!("Message: {:?}", text);
    } else {
        println!("No Message Found");
    }

    Ok(())
}
