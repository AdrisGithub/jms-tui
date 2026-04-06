use j4rs::{InvocationArg, JvmBuilder, MavenArtifact, errors::Result, Null};

fn main() -> Result<()> {
    let jvm = JvmBuilder::new().build()?;

    let dbx_artifact = MavenArtifact::from("org.apache.artemis:artemis-jakarta-client:2.53.0");
    jvm.deploy_artifact_and_deps(&dbx_artifact)?;

    let connection_factory = jvm.create_instance(
        "org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory",
        InvocationArg::empty(), 
    )?;

    let connection = jvm.invoke(
        &connection_factory, 
        "createConnection",  
        &[
            InvocationArg::try_from("artemis")?,
            InvocationArg::try_from("artemis")?,
        ], 
    )?;

    jvm.invoke(
        &connection,            
        "start",                
        InvocationArg::empty(), 
    )?;

    let session = jvm.invoke(
        &connection,            
        "createSession",        
        InvocationArg::empty(), 
    )?;

    let message = jvm.invoke(
        &session,                                       
        "createTextMessage", 
        &[InvocationArg::try_from("Hello from Rust")?], 
    )?;

    let queue = jvm.invoke(
        &session,                                          
        "createQueue", 
        &[InvocationArg::try_from("TEST::TEST_ANYCAST")?], 
    )?;

    let producer = jvm.invoke(
        &session,                           
        "createProducer",                   
        &[InvocationArg::from(queue)], 
    )?;

    jvm.invoke(
        &producer,                            
        "send",                               
        &[InvocationArg::from(message)], 
    )?;

    let queue = jvm.invoke(
        &session,                                    
        "createQueue",                               
        &[InvocationArg::try_from("TEST_ANYCAST")?], 
    )?;

    let consumer = jvm.invoke(
        &session,                           
        "createConsumer",                   
        &[InvocationArg::from(queue)], 
    )?;

    let message = jvm.invoke(
        &consumer,              
        "receiveNoWait",              
        InvocationArg::empty(), 
    )?;

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

    jvm.invoke(
        &session,
        "close",
        InvocationArg::empty(), 
    )?;

    jvm.invoke(
        &connection,
        "close",
        InvocationArg::empty(), 
    )?;

    jvm.invoke(
        &connection_factory,
        "close",
        InvocationArg::empty(), 
    )?;

    Ok(())
}
