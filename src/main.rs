use j4rs::{InvocationArg, JvmBuilder, MavenArtifact, errors::Result, Null, Jvm, Instance};

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

    fn receive_no_wait(&self) -> Result<Instance> {
        let message = self.jvm.invoke(
            &self.instance,
            "receiveNoWait",
            InvocationArg::empty(),
        )?;
        Ok(message)
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

    fn send(&self, message: Instance) -> Result<()> {
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

    fn create_text_message(&self, text: &str) -> Result<Instance> {
        let message = self.jvm.invoke(
            &self.instance,
            "createTextMessage",
            &[InvocationArg::try_from(text)?],
        )?;
        Ok(message)
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

    fn close(self) -> Result<()> {
        self.jvm.invoke(
            &self.instance,
            "close",
            InvocationArg::empty(),
        )?;
        Ok(())
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

    fn close(self) -> Result<()> {
        self.jvm.invoke(
            &self.instance,
            "close",
            InvocationArg::empty(),
        )?;
        Ok(())
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

    fn close(self) -> Result<()> {
        self.jvm.invoke(
            &self.instance,
            "close",
            InvocationArg::empty(),
        )?;
        Ok(())
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

    producer.send(message)?;

    let queue = session.create_queue("TEST_ANYCAST")?;

    let consumer = session.create_consumer(queue)?;

    let message = consumer.receive_no_wait()?;

    let is_null = jvm.check_equals(&message, InvocationArg::try_from(Null::Boolean)?)?;

    if is_null {
        println!("No Message Found");
    } else {
        let id = jvm.invoke(
            &message,               
            "getJMSMessageID",      
            InvocationArg::empty(), 
        )?;

        let id: String = jvm.to_rust(id)?;
        println!("{}", id);

        let class_name: String = jvm
            .chain(&message)?
            .cast("java.lang.Object")?
            .invoke("getClass", InvocationArg::empty())?
            .invoke("getName", InvocationArg::empty())?
            .to_rust()?;

        println!("{}", class_name);

        if class_name.contains("TextMessage") {
            let textmessage = jvm.cast(&message, "jakarta.jms.TextMessage")?;

            let text = jvm.invoke(
                &textmessage,               
                "getText",      
                InvocationArg::empty(), 
            )?;

            let text: String = jvm.to_rust(text)?;

            println!("{}", text);
        }
    }

    session.close()?;
    connection.close()?;
    connection_factory.close()?;

    Ok(())
}
