define(function () {
	return {
		version: "1.0.1",
		load: function (name, req, onLoad, config) {
			if (config.isBuild) {
				//don't do anything if this is a build, can't inline a web worker
				onLoad();
				return;
			}

			var url = req.toUrl(name);

			if (window.Worker) {
				onLoad(new Worker(url));
			} else {
				req(["worker-fake"], function () {
					onLoad(new Worker(url));
				});
			}
		}
	};
});