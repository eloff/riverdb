use async_trait::async_trait;

use crate::riverdb::pool::PostgresCluster;
use crate::riverdb::Result;

// static mut CLIENT_CONNECT_PLUGINS: Vec<Box<dyn ClientConnectPlugin + Sync + Send>> = Vec::new();
//
// pub struct ClientConnectContext {
//     plugins: &'static [Box<dyn ClientConnectPlugin + Sync + Send>],
//     index: usize,
// }
//
// impl ClientConnectContext {
//     pub fn new() -> Self {
//         Self {
//             plugins: unsafe { &CLIENT_CONNECT_PLUGINS[..] },
//             index: 0,
//         }
//     }
//
//     pub async fn next(&mut self, client: &mut PostgresSession) -> Result<&'static PostgresCluster> {
//         let i = self.index;
//         if i < self.plugins.len() {
//             self.index += 1;
//             self.plugins[i].client_connected(self, client).await
//         } else {
//             if i != self.plugins.len() {
//                 panic!("called next too many times");
//             }
//             self.index = usize::MAX;
//             client.client_connected(self).await
//         }
//     }
// }
//
// #[async_trait]
// pub trait ClientConnectPlugin {
//     async fn client_connected(&self, ctx: &mut ClientConnectContext, client: &mut PostgresSession) -> Result<&'static PostgresCluster>;
// }
//
// pub async fn run_client_connect_plugins(client: &mut PostgresSession) -> Result<&'static PostgresCluster> {
//     ClientConnectContext::new().next(client).await
// }

// Plugins is a list of River DB plugins. Plugins are invoked in order.
// If a plugin needs to be in a different order depending on the hook, split it into multiple plugins.
// install_plugins is called when the server starts. Afterwards it's an error to modify Plugins.
//
// Plugins implement interfaces for registering custom plugins
// which can "hook" into various stages of a Postgres session in order
// to inspect or modify the behavior. Some examples of things that can be done
// are data change notifications, caching, logging, partitioning, syncing, data pipelines,
// query rewriting, triggering events, authorization, security, etc.
//
// Generally plugins take arguments including the next plugin to call.
// By invoking the next plugin explicitly, any plugin may execute custom code
// before and after subsequent plugins have run, including the default behavior.
// They may suppress the default behavior by opting not to call the next plugin.
// They may also modify the arguments before passing them to the next plugin,
// but it's important to follow any specific rules there might be for how the
// arguments may be modified. See the documentation for te specific plugin interface.
// This is similar to how http middleware works in Go, or Express.
//
// If a plugin returns an error, that's returned to the caller of "hook" which
// will determine the course of action, usually to log it and terminate the session.
// If the plugin did not call the next plugin, no subsequent plugins, including
// the default behavior, are invoked. Think of the default behavior as being the
// last plugin in the list. For example, the default behavior for receiving a message
// from the client is parsing and further processing the message and forwarding it
// to the backend connection. A plugin that doesn't call next, prevents any of that
// from happening. Which might be desirable, if the plugin forwarded the message itself
// to the backend to bypass the default behavior.
//
// Some plugins also return a value. In this case the default behavior is to "create"
// that object, so there's no point in calling next to invoke further plugins or the default
// behavior. OnConnect and OnConnectBackend are examples of this kind of plugin.
//
// Check the documentation for each specific plugin about what to expect for the arguments.
// If a plugin inspects its arguments and determines that it need do nothing, it must
// just invoke next and return the result directly. Don't make the mistake of forgetting
// to call next, as subsequent plugins and the default behavior won't be invoked.
//
// Plugins are async, currently this requires use of the async_trait crate when defining the
// trait implementation. Becasue plugins are async, they should not block or take an unreasonable amount
// of time because they're blocking an entire reactor thread when executing. This is especially
// important for plugins that call external code like C, Python, JavaScript, etc. Also note that
// it doesn't matter if you use async code in Python or JavaScript, you have to essentially block
// on it's completion before the plugin returns, so it's just a more convoluted way of blocking.
// Use tokio spawn_blocking to convert blocking code to async by running it in a background
// thread pool.
//
// A plugin can have internal state, and can initialize it in OnLoadPlugins and OnUnloadPlugins.
// However, plugins may be called concurrently from multiple threads, so take care to synchronize
// access to internal state (they're marked Send+Sync, so rust will enforce this requirement.)
//
// If you make an open-source plugin and share it with the world, please submit a pull request
// to the plugins repository to update the list of external plugins. It is also possible
// to petition to have it included in the main plugins repository and be maintained by River DB.
// The criteria for that are that it would realistically benefit both the community and River DB,
// the company, sufficiently to be worth the maintenance burden. That typically means that the plugin
// must be widely used, largely finished (in maintenance mode rather than new development
// mode), and that the community would not be harmed by decreased competition in the domain
// and slower release cycles. When the community would benefit more from a canonical version of
// something and focusing effort behind that rather than faster development cycles or competition.
