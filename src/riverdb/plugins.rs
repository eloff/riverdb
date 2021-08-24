pub trait Plugin: Sized {
    fn new() -> &'static Self;

    fn order(&self) -> i32 { 0 }
}

pub static mut CONFIGURED_PLUGINS: bool = false;
static mut CONFIGURE_PLUGINS: Vec<unsafe fn()> = Vec::new();

pub unsafe fn register_plugin_definition(configure: unsafe fn()) {
    CONFIGURE_PLUGINS.push(configure);
}

pub unsafe fn configure() {
    for f in &CONFIGURE_PLUGINS {
        (*f)()
    }
    CONFIGURED_PLUGINS = true;
}

#[macro_export]
macro_rules! define_event {
    (
        $(#[$meta:meta])*
        $name:ident, ($event_src:ident: &$l:lifetime $src_ty:ty $(,$arg:ident: $arg_ty:ty)*) -> $result:ty
    ) => {
        $(#[$meta])*
        pub mod $name {
            pub use super::*;

            /// Source is the source type which triggers this event
            pub type Source = $src_ty;

            // This boxes the Future, which is unfortunate, but that restriction may be lifted in a future edition of Rust
            // We need to be able to return impl dyn Future here to avoid boxing.
            type Plugin<$l> = fn(ctx: &$l mut Event, $event_src: &$l Source, $($arg: $arg_ty),*) -> std::pin::Pin<Box<dyn std::future::Future<Output=$result> + Send + Sync + $l>>;

            // See notes on register for safety
            static mut PLUGINS: Vec<Plugin<'static>> = Vec::new();
            static mut PLUGINS_UNORDERED: Vec<(i32, Plugin<'static>)> = Vec::new();

            /// register globally registers a plugin function, it's called by async_plugin! before main() starts.
            /// It's an error to call this once plugins are configured.
            pub unsafe fn register(order: i32, f: Plugin<'static>) {
                assert!(!$crate::riverdb::plugins::CONFIGURED_PLUGINS);
                PLUGINS_UNORDERED.push((order, f));
            }

            /// clear all globally registered plugins. This is exposed for use in tests.
            /// It's an error to call this once plugins are configured.
            pub unsafe fn clear() {
                assert!(!$crate::riverdb::plugins::CONFIGURED_PLUGINS);
                PLUGINS.clear();
                PLUGINS_UNORDERED.clear();
            }

            /// configure is called after registering all plugins, but before they are used
            /// It's invoked after loading the configuration, but before starting the server.
            /// This is exposed for use in tests. It's an error to call this once plugins are configured.
            pub unsafe fn configure() {
                assert!(!$crate::riverdb::plugins::CONFIGURED_PLUGINS);
                // Sort the plugins by the order field in tuple index 0.
                PLUGINS_UNORDERED.sort_unstable_by_key(|(order,_)| *order);
                // Populate the PLUGINS Vec by the ordered plugins in tuple index 1.
                for (_, f) in PLUGINS_UNORDERED.drain(..) {
                    PLUGINS.push(f);
                }
            }

            #[ctor::ctor]
            unsafe fn register_plugin_configure() {
                $crate::riverdb::plugins::register_plugin_definition(configure);
            }

            pub struct Event{
                //data: EventData = Vec<(&'static str, ?> // optional key-value pairs
                index: usize,
            }

            impl Event {
                pub fn new() -> Self {
                    Self{index: 0}
                }

                /// next() invokes the next plugin in the chain, or the default behavior
                pub async fn next<$l>(&$l mut self, $event_src: &$l Source, $($arg: $arg_ty),*) -> $result {
                    let i = self.index;
                    let plugins = unsafe { &PLUGINS[..] };
                    if i < plugins.len() {
                        let plugin_fn: Plugin = unsafe {
                            // Transmute to change lifetime (including for the slice elements) here from 'static to one more restrictive
                            std::mem::transmute(*plugins.get_unchecked(i))
                        };
                        self.index = i + 1;
                        plugin_fn(self, $event_src, $($arg),*).await
                    } else if i != plugins.len() {
                        panic!("called next too many times (did you mean to clone() the context first?)");
                    } else {
                        self.index = i + 1;
                        $event_src.$name(self, $($arg),*).await
                    }
                }
            }

            /// run invokes the plugins registered in this module
            pub async fn run<$l>($event_src: &$l Source, $($arg: $arg_ty),*) -> $result {
                let mut ev = Event::new();
                // With this check, we can avoid allocating a boxed Future if there aren't any plugins registered
                if unsafe { PLUGINS.is_empty() } {
                    $event_src.$name(&mut ev, $($arg),*).await
                } else {
                    ev.next($event_src, $($arg),*).await
                }
            }
        }
    }
}

/// async_plugin! registers the passed async handler for the defined plugin module.
///     $event_name:ident : the module defining the plugin hook we want to register for
///     $l:lifetime : a named lifetime for reference arguments captured for the duration of the async plugin invocation
///     $event:ident : the name of local holding a mut ref to the $event_name::Event context instance
///     $src:ident : the name of the local holding a mut ref to the $event_name::Source instance that triggered the event
///     $p:ident : the name of the local holding a static shared ref to the $plugin_type singleton
///     $plugin_type:ty : the name of a singleton type that can store configuration settings and state for the plugin
///     ($($arg:ident: $arg_ty:ty),*) : a list of arguments and types passed into the event, must exactly match the arguments in the plugin definition
///     $result:ty : the return type of the plugin, must exactly match the return type in the plugin definition
///     $body:block : the async plugin code block which is wrapped as an async move block capturing the locals and arguments above
///
/// Plugins "hook" into various stages of a Postgres session in order
/// to inspect or modify the behavior. Some examples of things that can be done
/// are data change notifications, caching, logging, partitioning, syncing, data pipelines,
/// query rewriting, triggering events, authorization, security, etc. Plugins are
/// Rust code, but that Rust code can call C, JavaScript, Python, etc.
///
/// River DB plugins must currently be statically compiled into River DB itself.
/// While this sounds onerous, this enforces that the same compiler is used for both
/// and ensures everything is checked by the compiler. Rust does not yet have a stable ABI,
/// so the dynamic linking story is something of a nightmare. A silver lining is
/// plugins can take full advantage of inlining and link time optimizations in LLVM
/// and run incredibly efficiently.
///
/// Plugins are invoked in configuration file order, so it's good practice to list all plugins
/// in the config file, even if you don't need to override any configuration options for them.
/// The key for the plugin in the config file is the name of the plugin type passed to async_plugin!.
/// If a plugin type is used for multiple events, and it needs to be in a different order depending
/// on which event is being handled, you can override the numeric order on a per-event basis
/// in the config file.
///
/// Generally plugins take arguments including an Event context object containing
/// an async next(...) method which invokes the next plugins and/or default behavior.
/// By invoking the next plugin explicitly, any plugin may execute custom code
/// both before and after subsequent plugins have run. They may completely replace
/// the default behavior by opting not to call next.
/// This is similar to how http middleware works in Go, or Express.
/// The context can be cloned, which can allow calling the next plugins/default behavior
/// multiple times - where that makes sense. See the documentation for the specific plugin module.
/// Don't make the mistake of forgetting to call next, as subsequent plugins
/// and the default behavior won't be invoked.
///
/// If a plugin returns an error, that's returned to the caller of "hook" which
/// will determine the course of action, usually to log it and terminate the session.
/// If the plugin did not call the next plugin, no subsequent plugins, including
/// the default behavior, are invoked. Think of the default behavior as being the
/// last plugin in the list. For example, the default behavior for receiving a message
/// from the client is parsing and further processing the message and forwarding it
/// to the backend connection. A plugin that doesn't call next, prevents any of that
/// from happening. Which might be desirable, if the plugin forwarded the message itself
/// to the backend to bypass the default behavior.
///
/// Some plugins also return a value. In this case the default behavior is to create
/// that object, so once you've done that, there's no point in calling next to invoke
/// further plugins or the default behavior.
///
/// Plugins are async so they should not block or take an unreasonable amount
/// of time because they're blocking an entire reactor thread when executing. This is especially
/// important for plugins that call external code like C, Python, JavaScript, etc. Also note that
/// it doesn't matter if you use async/await in Python or JavaScript, if the ffi layer is not async.
/// Use tokio spawn_blocking to convert blocking code to async by running it in a background thread pool.
///
/// A plugin can have internal state, behind a shared reference and can initialize it in configure_plugin.
/// However, plugins may be called concurrently from multiple threads, so take care to synchronize
/// access to internal state (they're marked Send+Sync, so rust will enforce this requirement.)
///
/// If you make an open-source plugin and share it with the world, please submit a pull request
/// to the plugins repository to update the list of community plugins.
#[macro_export]
macro_rules! event_listener {
    ($plugin:expr, $plugin_type:ident : $event_name:ident<$l:lifetime>($($arg:ident: $arg_ty:ty),*) -> $result:ty) => {
        {
            #[allow(non_upper_case_globals)]
            static $plugin_type: std::sync::atomic::AtomicPtr<$plugin_type> = std::sync::atomic::AtomicPtr::new(std::ptr::null_mut());

            fn _plugin_fn<$l>(ev: &$l mut $event_name::Event, src: &$l $event_name::Source, $($arg: $arg_ty),*)
                -> std::pin::Pin<Box<dyn std::future::Future<Output=$result> + Send + Sync + $l>>
            {
                let p = unsafe { &*$plugin_type.load(std::sync::atomic::Ordering::Relaxed) };
                Box::pin(p.$event_name(ev, src, $($arg),*))
            }

            $plugin_type.store($plugin as *const $plugin_type as *mut $plugin_type, std::sync::atomic::Ordering::Relaxed);
            unsafe {
                $event_name::register($plugin.order(), _plugin_fn);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use super::{Plugin, configure};
    
    use crate::riverdb::Result;

    pub struct RecordMonitor(Mutex<RecordMonitorState>);

    pub struct RecordMonitorState {
        greeting: String,
        state: i32
    }

    impl RecordMonitor {
        async fn record_changed(&self, _ev: &mut record_changed::Event, payload: &str) -> Result<String> {
            Ok(payload.to_lowercase() + &self.0.lock().unwrap().greeting)
        }
    }

    define_event!(record_changed, (monitor: &'a RecordMonitor, payload: &'a str) -> Result<String>);

    struct Listener2 {
        foo: i32,
        bar: i32
    }

    impl Listener2 {
        pub async fn record_changed(&self, ev: &mut record_changed::Event, monitor: &RecordMonitor, payload: &str) -> Result<String> {
            monitor.0.lock().unwrap().state += self.bar;
            let s = "-2b-".to_string() + &ev.next(monitor, payload).await? + "-2a-";
            monitor.0.lock().unwrap().state *= self.bar;
            Ok(s)
        }
    }

    impl Plugin for Listener2 {
        fn new() -> &'static Self {
            Box::leak(Box::new(Self{foo: 0, bar: 5}))
        }

        fn order(&self) -> i32 {
            2
        }
    }

    struct Listener {
        foo: i32,
        bar: i32
    }

    impl Listener {
        pub async fn record_changed(&self, ev: &mut record_changed::Event, monitor: &RecordMonitor, payload: &str) -> Result<String> {
            monitor.0.lock().unwrap().state += self.foo;
            let s = "-1b-".to_string() + &ev.next(monitor, payload).await? + "-1a-";
            monitor.0.lock().unwrap().state *= self.foo;
            Ok(s)
        }
    }

    impl Plugin for Listener {
        fn new() -> &'static Self {
            Box::leak(Box::new(Self{foo: 3, bar: -1}))
        }

        fn order(&self) -> i32 {
            1
        }
    }

    #[tokio::test]
    async fn test_event() {
        let p1 = Listener2::new();
        event_listener!(p1, Listener2:record_changed<'a>(payload: &'a str) -> Result<String>);
        event_listener!(Listener::new(), Listener:record_changed<'a>(payload: &'a str) -> Result<String>);

        unsafe {
            configure();
        }

        let monitor = RecordMonitor(Mutex::new(RecordMonitorState{ greeting: " world!".to_string(), state: 1 }));
        let result = record_changed::run(&monitor, "HELLO").await;
        assert_eq!(Ok("-1b--2b-hello world!-2a--1a-".to_string()), result);
        assert_eq!(monitor.0.lock().unwrap().state, (1+3+5)*5*3);
    }
}
