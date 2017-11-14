define('palette',[],function () {
  function path(points, command, close) {
    return [
      "M",
      [points[0][0], -points[0][1]].join(','),
      command,
      points.slice(1, points.length)
        .map(function (e) {
          return [e[0], -e[1]].join(",");
        }), close===true ? "Z" : ""
    ].join(" ").trim();
  }

  return {
    polygon: function (d) {
      return path(d.points, "L", true);
    },
    ellipse: function (d) {
      return [
        'M', [d.cx, -d.cy].join(','),
        'm', [-d.rx, 0].join(','),
        'a', [d.rx, d.ry].join(','),
        0, "1,0", [2 * d.rx, 0].join(','),
        'a', [d.rx, d.ry].join(','),
        0, "1,0", [-2 * d.rx, 0].join(',')].join(' ');
    },
    circle: function (d) {
      return this.ellipse({
        cx: d.cx,
        cy: -d.cy,
        rx: d.r,
        ry: d.r
      });
    },
    rect: function (d) {
      return this.polygon({
        points: [
          [d.x, -d.y],
          [d.x + d.width, -d.y],
          [d.x + d.width, -d.y + d.height],
          [d.x, -d.y + d.height]
        ]
      });
    },
    path: function (d) {
      return path(d.points, "C");
    },
    bspline: function (d) {
      return path(d.points, "C");
    },
    polyline: function (d) {
      return path(d.points, "L");
    }
  };
});
define('styliseur',["d3"], function(d3) {
  var styliseur = function () {
    this.each(function (d) {
      var self = d3.select(this);
      var fillInsteadOfStroke = this instanceof SVGTextElement || false;
      d && d.style && d.style.forEach(function (e) {
        switch (e.key) {
          case "stroke":
          case "fill":
            var attribute = e.key==="stroke" && fillInsteadOfStroke ? "fill" : e.key;
            var transparent = e.value.indexOf("#")===0 &&  e.value.length === 9;
            var color = transparent ? e.value.substr(0,7) : e.value;
            var opacity = transparent ? parseInt(e.value.substr(7,2),16)/255 : 1;
            self.attr(attribute, color);
            opacity < 1 && self.attr(attribute + "-opacity", opacity);
            break;
          case "font-size":
          case "font-family":
            self.attr(e.key, e.value);
            break;
          case "style":
            if (e.value.indexOf('setline') === 0) {
              self.attr('stroke-width', 2);
            } else {
              self.attr('class', e.value);
            }
        }
      });
    });
  };

  return styliseur;
});
define('transitions/default',["styliseur"], function(styliseur) {
  return {
    document: function(selection, attributer) {
      selection
        .transition()
        .delay(150)
        .duration(700)
        .call(attributer);
    },
    canvas: function (selection, attributer) {
      selection
        .transition()
        .delay(150)
        .duration(900)
        .call(attributer)
        .call(styliseur);
    },
    nodes: function (selection, attributer) {
      selection
        .style("opacity", 0.0)
        .transition()
        .delay(150)
        .duration(900)
        .call(attributer)
        .call(styliseur);
    },
    relations: function (selection, attributer) {
      selection
        .style("opacity", 0.0)
        .transition()
        .delay(150)
        .duration(900)
        .call(attributer)
        .call(styliseur);
    },
    exits: function (selection, attributer) {
      selection
        .transition()
        .duration(100)
        .style("opacity", 0.0)
        .call(attributer);
    },
    shapes: function (shapes, attributer) {
      shapes
        .transition()
        .delay(150)
        .duration(900)
        .call(attributer)
        .call(styliseur);
    },
    labels: function (labels, attributer) {
      labels
        .transition()
        .delay(150)
        .duration(900)
        .call(attributer)
        .call(styliseur);
    }
  };
});
define('stage',["d3", "palette", "transitions/default"], function (d3, palette, defaults) {
    var svg, main, zoom;
    var order = {
      digraph: 0,
      subgraph: 1,
      node: 2,
      relation: 3
    };
    var transitions = defaults;

    function calculateSizes(main) {
      var margin = 4,
        boundingWidth = main.shapes[0].points[2][0] + margin * 2,
        boundingHeight = main.shapes[0].points[2][1] + margin * 2,
        htranslate = main.shapes[0].points[2][1] + margin,
        vtranslate = margin,
        size = main.size || [boundingWidth, boundingHeight];
      var oversized = boundingWidth > size[0] || boundingHeight > size[1];
      var scaleWidth = oversized ? size[0] / boundingWidth : 1;
      var scaleHeight = oversized ? size[1] / boundingHeight : 1;
      var ratio = scaleHeight > scaleWidth ? scaleHeight = scaleWidth : scaleWidth = scaleHeight;
      var height = boundingHeight * ratio;
      var width = boundingWidth * ratio;

      return {
        boundingWidth: boundingWidth,
        boundingHeight: boundingHeight,
        margin: margin,
        width: width,
        height: height,
        htranslate: htranslate,
        vtranslate: vtranslate,
        scaleWidth: scaleWidth,
        scaleHeight: scaleHeight,
        style: main.shapes[0].style
      };
    }

    var labelAttributer = function () {
      this
        .attr("x", function (d) {
          return d.x;
        })
        .attr("y", function (d) {
          return -d.y;
        })
        .text(function (d) {
          return d.text;
        })
        .attr("text-anchor", function(d){
          return d.anchor;
        });
    };

    function zoomed() {
      svg.select("g")
        .attr('transform', 'translate(' + d3.event.translate + ') scale(' + d3.event.scale + ')');
    }

    return {
      init: function (definition) {
        var element = definition.element || definition;
        svg = d3.select(element).append("svg")
          .attr("version", 1.1)
          .attr("xmlns", "http://www.w3.org/2000/svg");
        svg.append("style")
          .attr("type", "text/css")
          .text([
            '.dashed {stroke-dasharray: 5,5}',
            '.dotted {stroke-dasharray: 1,5}',
            '.overlay {fill: none; pointer-events: all}'
          ].join(' '));
        main = svg.append("g").append("g");
        main.append("polygon").attr("stroke", {red: 255, green: 255, blue: 255, opacity: 0});

        if (definition.zoom) {
          var extent = definition.zoom && definition.zoom.extent || [0.1, 10];
          zoom = d3.behavior
            .zoom()
            .scaleExtent(extent)
            .on("zoom", zoomed);

          svg.select("g")
            .call(zoom)
            .append("rect")
            .attr("class", "overlay");
        }
      },
      svg: function (reset) {
        if (reset) {
          var g = svg.select("g").select("g");
          if (g[0]) {
            zoom.scale(1);
            zoom.translate([0, 0]);
            g.attr("transform", "translate(0,0)scale(1)");
          }
        }
        return svg.node().parentNode.innerHTML;
      },
      setZoom: function (zoomParams) {
        zoomParams.scale && zoom.scale(zoomParams.scale);
        zoomParams.translate && zoom.translate(zoomParams.translate);
        zoom.event(svg);
      },
      transitions: function (custom) {
        if (custom) {
          transitions = custom;
        } else {
          return transitions;
        }
      },
      draw: function (stage) {
        var sizes = calculateSizes(stage.main);
        var area = [0, 0, sizes.width, sizes.height];

        transitions.document(svg, function () {
          this
            .attr("width", sizes.width + "pt")
            .attr("height", sizes.height + "pt")
            .attr("viewBox", area.join(' '))
            .select("g")
            .select("g")
            .attr("transform", "scale(" + sizes.scaleWidth + " " + sizes.scaleHeight + ")" +
              " " + "translate(" + sizes.vtranslate + "," + sizes.htranslate + ")");
        });

        var polygon = main.select("polygon").data(stage.main.shapes);
        transitions.canvas(polygon, function () {
          this
            .attr("points", function () {
              return [
                [-sizes.margin, sizes.margin],
                [-sizes.margin, -sizes.boundingHeight],
                [sizes.boundingWidth, -sizes.boundingHeight],
                [sizes.boundingWidth, sizes.margin]]
                .map(function (e) {
                  return e.join(",");
                }).join(" ");
            });
        });

        var overlay = svg.select("rect.overlay");
        if (overlay[0]) {
          transitions.canvas(overlay, function () {
            this
              .attr('width', sizes.width / sizes.scaleWidth)
              .attr('height', sizes.height / sizes.scaleHeight)
              .attr('y', -sizes.height / sizes.scaleHeight);
          });
        }
        var label = main.selectAll("text")
          .data(stage.main.labels);
        label.enter().append("text");
        transitions.labels(label, labelAttributer);

        var groups = main.selectAll("g")
          .data(stage.groups, function (d) {
            return d.id;
          });
        var entering = groups.enter()
          .append("g").attr("class", function (d) {
            return d.class;
          });
        entering.append("title").text(function (d) {
          return d.id;
        });

        entering
          .filter(function(d) { return d.url; })
          .append("a")
          .attr("xlink:href", function(d) {return d.url;})
          .attr("xlink:title", function(d) {return d.tooltip;});

        transitions.nodes(entering.filter(".node:empty,.node a"), function () {
          this.style("opacity", 1.0);
        });
        transitions.relations(entering.filter(".relation"), function () {
          this.style("opacity", 1.0);
        });
        transitions.exits(groups.exit(), function () {
          this.remove();
        });

        groups.sort(function (a, b) {
          return order[a.class] - order[b.class];
        });

        var leaves = main
          .selectAll("*")
          .filter(function(d) {
            return this instanceof SVGAElement ||
              (this instanceof SVGGElement && !this.querySelector("a"));
          });
        var shapes = leaves
          .selectAll("path")
          .data(function (d) {
            return d.shapes;
          }, function (d, i) {
            return [d.shape, i].join('-');
          }
        );
        shapes.enter().append("path");
        transitions.shapes(shapes, function () {
          this
            .attr("d", function (d) {
              var shape = d.shape;
              return palette[shape](d);
            });
        });

        var labels = leaves
          .selectAll("text")
          .data(function (d) {
            return d.labels;
          });
        labels.enter().append("text");
        transitions.labels(labels, labelAttributer);
      },
      getImage: function (reset) {
        reset = reset === undefined ? true : reset;
        var svgXml = this.svg(reset);
        var scaleFactor = 1;

        var svgImage = new Image();
        var pngImage = new Image();

        svgImage.onload = function () {
          var canvas = document.createElement("canvas");
          canvas.width = svgImage.width * scaleFactor;
          canvas.height = svgImage.height * scaleFactor;

          var context = canvas.getContext("2d");
          context
            .drawImage(
              svgImage, 0, 0, canvas.width, canvas.height);

          pngImage.src = canvas.toDataURL("image/png");
          pngImage.width = svgImage.width;
          pngImage.height = svgImage.height;
        };

        svgImage.src = "data:image/svg+xml;base64," + btoa(svgXml);
        return pngImage;
      }
    };
  }
);
define('worker',[],function () {
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

define('renderer',["stage", "worker!layout-worker.js"], function(stage, worker) {

  var initialized = false, pending, errorCallback, renderCallback;

  worker.onmessage = function (event) {
    switch (event.data.type) {
      case "ready":
        initialized = true;
        if (pending) {
          worker.postMessage(pending);
        }
        break;
      case "stage":
        stage.draw(event.data.body);
        renderCallback && renderCallback();
        break;
      case "error":
        if (errorCallback) {
          errorCallback(event.data.body);
        }
    }
  };

  return {
    init: function(element) {
      return stage.init(element);
    },
    render: function(source) {
      if (initialized) {
        worker.postMessage(source);
      } else {
        pending = source;
      }
    },
    stage: stage,
    errorHandler: function(handler) {
      errorCallback = handler;
    },
    renderHandler: function(handler) {
      renderCallback = handler;
    }
  };

});
