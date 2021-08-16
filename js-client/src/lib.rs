#![warn(clippy::dbg_macro)]
#![allow(dead_code)]

mod convert;
mod utils;

use neon::prelude::*;
use std::cell::RefCell;
use tokio::runtime::Runtime;

use nom_sql::SelectStatement;
use noria::{ControllerHandle, DataType, ZookeeperAuthority};
use noria_client::backend::{
    mysql_connector::MySqlConnector, noria_connector::NoriaConnector, Backend, BackendBuilder,
    Reader, Writer,
};
use std::collections::{HashMap, HashSet};
use std::sync::atomic::AtomicUsize;
use std::sync::{Arc, RwLock};

type BoxedClient = JsBox<RefCell<JsClient>>;

struct JsClient {
    backend: Arc<tokio::sync::Mutex<Backend<ZookeeperAuthority>>>,
    runtime: Runtime,
}

impl Finalize for JsClient {}

impl JsClient {
    pub fn new(b: Backend<ZookeeperAuthority>, rt: Runtime) -> Self {
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
    let static_responses = parse_arg!("staticResponses", true, JsBoolean);
    let slowlog = parse_arg!("slowLog", false, JsBoolean);
    let permissive = parse_arg!("permissive", false, JsBoolean);
    let read_your_write = parse_arg!("readYourWrite", false, JsBoolean);
    let region = parse_arg!("region", "".to_string(), JsString);

    let rt = tokio::runtime::Runtime::new().unwrap();
    let zk_auth = ZookeeperAuthority::new(&format!("{}/{}", zk_addr, deployment)).unwrap();
    let ch = rt.block_on(ControllerHandle::new(zk_auth));
    let auto_increments: Arc<RwLock<HashMap<String, AtomicUsize>>> = Arc::default();
    let query_cache: Arc<RwLock<HashMap<SelectStatement, String>>> = Arc::default();
    let writer = if !mysql_address.is_empty() {
        let writer = rt.block_on(MySqlConnector::new(mysql_address.clone()));
        Writer::MySqlConnector(writer)
    } else {
        let writer = rt.block_on(NoriaConnector::new(
            ch.clone(),
            auto_increments.clone(),
            query_cache.clone(),
            Some(region.clone()),
        ));
        Writer::NoriaConnector(writer)
    };
    let noria_connector = rt.block_on(NoriaConnector::new(
        ch,
        auto_increments,
        query_cache,
        Some(region),
    ));
    let mysql_connector = if !mysql_address.is_empty() {
        Some(rt.block_on(MySqlConnector::new(mysql_address)))
    } else {
        None
    };

    let reader = Reader {
        mysql_connector,
        noria_connector,
    };
    let b = BackendBuilder::new()
        .static_responses(static_responses)
        .slowlog(slowlog)
        .permissive(permissive)
        .require_authentication(false)
        .enable_ryw(read_your_write)
        .build(writer, reader);

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
                    convert::convert_error(&mut cx, e)?.upcast::<JsValue>(),
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

fn async_execute(mut cx: FunctionContext) -> JsResult<JsUndefined> {
    let wrapped_jsclient = cx.argument::<BoxedClient>(0)?;
    let jsclient = wrapped_jsclient.borrow_mut();
    let statement_id = cx.argument::<JsNumber>(1)?.value(&mut cx) as u32;
    let params: Vec<DataType> = cx
        .argument::<JsArray>(2)?
        .to_vec(&mut cx)?
        .iter()
        .map(|p| convert::convert_param(&mut cx, p))
        .collect::<Result<Vec<_>, _>>()?;
    let callback = cx.argument::<JsFunction>(3)?.root(&mut cx);
    let queue = cx.queue();
    let backend = jsclient.backend.clone();

    jsclient.runtime.spawn(async move {
        let res = backend.lock().await.execute(statement_id, params).await;

        queue.send(move |mut cx| {
            let (js_err, js_res) = match res {
                Ok(raw_query_result) => (
                    cx.null().upcast::<JsValue>(),
                    convert::convert_query_result(&mut cx, raw_query_result)?.upcast::<JsValue>(),
                ),
                Err(e) => (
                    convert::convert_error(&mut cx, e)?.upcast::<JsValue>(),
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
                    convert::convert_error(&mut cx, e)?.upcast::<JsValue>(),
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
    cx.export_function("asyncExecute", async_execute)?;
    cx.export_function("asyncQuery", async_query)?;
    Ok(())
}
