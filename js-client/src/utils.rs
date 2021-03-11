use neon::prelude::*;

pub(crate) fn set_str_field<'a, C>(
    cx: &mut C,
    obj: &Handle<JsObject>,
    key: &str,
    s: &str,
) -> NeonResult<()>
where
    C: Context<'a>,
{
    let key = cx.string(key);
    let val = cx.string(s);
    obj.set(cx, key, val)?;
    Ok(())
}

pub(crate) fn set_num_field<'a, C>(
    cx: &mut C,
    obj: &Handle<JsObject>,
    key: &str,
    i: f64,
) -> NeonResult<()>
where
    C: Context<'a>,
{
    let key = cx.string(key);
    let val = cx.number(i);
    obj.set(cx, key, val)?;
    Ok(())
}

pub(crate) fn set_jsval_field<'a, C>(
    cx: &mut C,
    obj: &Handle<JsObject>,
    key: &str,
    v: Handle<JsValue>,
) -> NeonResult<()>
where
    C: Context<'a>,
{
    let key = cx.string(key);
    obj.set(cx, key, v)?;
    Ok(())
}
