#![allow(dead_code)]

mod convert;
mod utils;

use neon::prelude::*;
use std::cell::RefCell;
use tokio::runtime::Runtime;

use nom_sql::SelectStatement;
use noria::{ControllerHandle, ZookeeperAuthority};
use noria_client::backend::{
    mysql_connector::MySqlConnector, noria_connector::NoriaConnector, Backend, BackendBuilder,
    Writer,
};
use std::collections::{HashMap, HashSet};
use std::sync::atomic::AtomicUsize;
use std::sync::{Arc, RwLock};

type BoxedClient = JsBox<RefCell<JsClient>>;

struct JsClient {
    backend: Arc<tokio::sync::Mutex<Backend>>,
    runtime: Runtime,
}

impl Finalize for JsClient {}

impl JsClient {
    pub fn new(b: Backend, rt: Runtime) -> Self {
        JsClient {
            backend: Arc::new(tokio::sync::Mutex::new(b)),
            runtime: rt,
        }
    }
}

fn connect(mut cx: FunctionContext) -> JsResult<BoxedClient> {
    let js_config = cx.argument::<JsObject>(0)?;
    let js_prop_names = js_config.get_own_property_names(&mut cx)?.to_vec(&mut cx)?;

    let prop_names = js_prop_names
        .iter()
        .map(|js_prop| Ok(js_prop.to_string(&mut cx).unwrap().value(&mut cx)))
        .collect::<Result<HashSet<_>, _>>()?;

    #[macro_export]
    macro_rules! parse_arg {
        ( $name:literal, $default:expr, $type:ty ) => {{
            if prop_names.contains($name) {
                let zk_key = cx.string($name);
                js_config
                    .get(&mut cx, zk_key)?
                    .downcast_or_throw::<$type, FunctionContext>(&mut cx)?
                    .value(&mut cx)
            } else {
                $default
            }
        }};
    }

    let deployment = parse_arg!("deployment", "myapp".to_string(), JsString);
    let zk_addr = parse_arg!("zookeeperAddress", "127.0.0.1:2181".to_string(), JsString);
    let mysql_address = parse_arg!("mySQLAddress", "".to_string(), JsString);
    let sanitize = parse_arg!("sanitize", true, JsBoolean);
    let static_responses = parse_arg!("staticResponses", true, JsBoolean);
    let slowlog = parse_arg!("slowLog", false, JsBoolean);
    let permissive = parse_arg!("permissive", false, JsBoolean);
    let read_your_write = parse_arg!("readYourWrite", false, JsBoolean);

    let mut rt = tokio::runtime::Runtime::new().unwrap();
    let zk_auth = ZookeeperAuthority::new(&format!("{}/{}", zk_addr, deployment)).unwrap();
    let ch = rt.block_on(ControllerHandle::new(zk_auth)).unwrap();
    let auto_increments: Arc<RwLock<HashMap<String, AtomicUsize>>> = Arc::default();
    let query_cache: Arc<RwLock<HashMap<SelectStatement, String>>> = Arc::default();
    let writer = if !mysql_address.is_empty() {
        let writer = rt.block_on(MySqlConnector::new(mysql_address));
        Writer::MySqlConnector(writer)
    } else {
        let writer = rt.block_on(NoriaConnector::new(
            ch.clone(),
            auto_increments.clone(),
            query_cache.clone(),
        ));
        Writer::NoriaConnector(writer)
    };
    let reader = rt.block_on(NoriaConnector::new(ch, auto_increments, query_cache));

    let b = BackendBuilder::new()
        .sanitize(sanitize)
        .static_responses(static_responses)
        .writer(writer)
        .reader(reader)
        .slowlog(slowlog)
        .permissive(permissive)
        .require_authentication(false)
        .enable_ryw(read_your_write)
        .build();

    let jsclient = RefCell::new(JsClient::new(b, rt));
    Ok(cx.boxed(jsclient))
}

fn async_prepare(mut cx: FunctionContext) -> JsResult<JsUndefined> {
    let wrapped_jsclient = cx.argument::<BoxedClient>(0)?;
    let jsclient = wrapped_jsclient.borrow_mut();
    let query = cx.argument::<JsString>(1)?.value(&mut cx);
    let callback = cx.argument::<JsFunction>(2)?.root(&mut cx);
    let queue = cx.queue();
    let backend = jsclient.backend.clone();

    jsclient.runtime.spawn(async move {
        let res = backend.lock().await.prepare(&query).await;

        queue.send(move |mut cx| {
            let (js_err, js_res) = match res {
                Ok(raw_prepare_result) => (
                    cx.null().upcast::<JsValue>(),
                    convert::convert_prepare_result(&mut cx, raw_prepare_result)?
                        .upcast::<JsValue>(),
                ),
                Err(e) => (
                    convert::convert_error(e)?.upcast::<JsValue>(),
                    cx.null().upcast::<JsValue>(),
                ),
            };
            let callback = callback.into_inner(&mut cx);
            let this = cx.undefined();
            let args = vec![js_err.upcast::<JsValue>(), js_res.upcast::<JsValue>()];
            callback.call(&mut cx, this, args)?;
            Ok(())
        })
    });

    Ok(cx.undefined())
}

// TODO: implement async_execute once ParamParser changes have been made.

fn async_query(mut cx: FunctionContext) -> JsResult<JsUndefined> {
    let wrapped_jsclient = cx.argument::<BoxedClient>(0)?;
    let jsclient = wrapped_jsclient.borrow_mut();
    let query = cx.argument::<JsString>(1)?.value(&mut cx);
    let callback = cx.argument::<JsFunction>(2)?.root(&mut cx);
    let queue = cx.queue();
    let backend = jsclient.backend.clone();

    jsclient.runtime.spawn(async move {
        let res = backend.lock().await.query(&query).await;

        queue.send(move |mut cx| {
            let (js_err, js_res) = match res {
                Ok(raw_query_result) => (
                    cx.null().upcast::<JsValue>(),
                    convert::convert_query_result(&mut cx, raw_query_result)?.upcast::<JsValue>(),
                ),
                Err(e) => (
                    convert::convert_error(e)?.upcast::<JsValue>(),
                    cx.null().upcast::<JsValue>(),
                ),
            };
            let callback = callback.into_inner(&mut cx);
            let this = cx.undefined();
            let args = vec![js_err, js_res];
            callback.call(&mut cx, this, args)?;
            Ok(())
        })
    });

    Ok(cx.undefined())
}

#[neon::main]
fn main(mut cx: ModuleContext) -> NeonResult<()> {
    cx.export_function("connect", connect)?;
    cx.export_function("asyncPrepare", async_prepare)?;
    cx.export_function("asyncQuery", async_query)?;
    Ok(())
}
