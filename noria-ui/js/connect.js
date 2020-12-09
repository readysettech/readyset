      var controller_addr = localStorage.getItem("noria_controller_addr");

      function connect(controller_addr) {
        localStorage.setItem("noria_controller_addr", controller_addr);
        $("#controller-addr-edit").addClass("d-none");
        $("#controller-addr").removeClass("d-none");
        $("#controller-addr").html("Connected to Noria controller at: <b>" + controller_addr + "</b>");
      }

      /* Controller connection handling */
      if (controller_addr) {
        $("#controller-addr").html("Connected to Noria controller at: <b>" + controller_addr + "</b>");
      } else {
        $("#controller-addr").html("<b>Click to connect to controller</b>");
      }

      $("#controller-addr").click(function() {
        $("#controller-addr").addClass("d-none");
        if (controller_addr) {
          $("#controller-addr-input").val(controller_addr);
        } else {
          $("#controller-addr-input").val("localhost:6033");
        }
        $("#controller-addr-edit").removeClass("d-none");
        $("#controller-addr-input").focus();
      });
      $("#controller-addr-connect").click(function() {
        controller_addr = $("#controller-addr-input").val();
        connect(controller_addr);
      });
      $("#controller-addr-input").keyup(function (evt) {
        if (evt["which"] == 13) {
          // enter/return
          controller_addr = $("#controller-addr-input").val();
          connect(controller_addr);
        }
      });

