// Developing this in jsfiddle: http://jsfiddle.net/bqk6pmjg/

var rootGroup = {
    type: "group",
    val: "all",
    queryGroup: []
};
  
function appendInnerSearchQuery(element, level, query, parentGroup, afterMode) {
    "use strict";

	var append = true;
    if (typeof afterMode !== 'undefined' || afterMode) {
        append = false;
    }

    var myDiv = document.createElement("div");
    myDiv.className = "level-" + level
  
  
    if (('' + query.type) === 'group') {
  
      // Group type select list
      var grouptypes = ["all", "any"];
          var select = document.createElement("select");
      select.name = "grouptype";
      for (const val of grouptypes)
      {
          var option = document.createElement("option");
          option.value = val;
          option.text = val;
          select.appendChild(option);
      }
      select.value = query.val;
      select.addEventListener("change", function(event) {
          var label = document.getElementById("formerror");
          label.innerHTML = "";
          query.val = event.target.value;
      });
      var label1 = document.createElement("label");
      label1.innerHTML = "Match "
      label1.htmlFor = "grouptype";
      myDiv.appendChild(label1);
      myDiv.appendChild(select);
  
    } else if (('' + query.type) === 'query') {

		var templateDiv = document.querySelector('#id_empty_form');
    	var elements = templateDiv.querySelectorAll("input,select,textarea,label,div");
        let clones = [];
        elements.forEach(function(elem) {
        	clones.push(elem.cloneNode(true));
        });
    	
        for (let i = 0; i < clones.length; i++) {
        	
        	// Dismiss any previous error (that was prevented)
            clones[i].addEventListener("click", function(event) {
                var label = document.getElementById("formerror");
                label.innerHTML = "";
            });
            
            // Keep the value up to date
            clones[i].addEventListener("change", function() {
            	query[clones[i].name] = event.target.value;
            });
            
            // Initialize the value in the hierarchy with the default
            query[clones[i].name] = clones[i].value;
            
            // Add this row to the HTML form
            myDiv.appendChild(clones[i]);
        }
        
    } else {
        var label = document.getElementById("formerror");
        label.innerHTML = "Error: Unrecognized query type: " + query.type;
    }
  
    if (level > 0) {
        var rmBtn = document.createElement("input");
        rmBtn.type = "button";
        rmBtn.value = "-";
        rmBtn.addEventListener("click", function(event) {
            var label = document.getElementById("formerror");
            label.innerHTML = "";

            var size = parentGroup.queryGroup.length;
            if (size <= 1) {
                var label = document.getElementById("formerror");
                label.innerHTML = "A group must have at least 1 query.";
            } else {
                event.target.parentNode.remove();
                var index = parentGroup.queryGroup.indexOf(query);
                parentGroup.queryGroup.splice(index, 1);
            }
        });
      myDiv.appendChild(rmBtn);
    }
    
    if (append) {
	    element.appendChild(myDiv);
    } else {
        element.after(myDiv);
    }
  
  
    if (('' + query.type) === 'group') {
  
        // Add a couple queries to start off
        var subQuery = {
            type: "query",
            val: ""
        }
        query.queryGroup.push(subQuery);
        appendInnerSearchQuery(myDiv, level + 1, subQuery, query);

        // If this isn't the root level, append a second
        if (level > 0) {
            var subQuery2 = {
                type: "query",
                val: ""
            }
            query.queryGroup.push(subQuery2);
            appendInnerSearchQuery(myDiv, level + 1, subQuery2, query);
        }
        
        myDiv.append(document.createElement("div"));

        if (level > 0) {

            // Add query to a group (button)
            var termbtn = document.createElement("input");
            termbtn.type = "button";
            termbtn.value = "+";
            termbtn.addEventListener("click", function(event) {
                var label = document.getElementById("formerror");
                label.innerHTML = "";

                var sibQuery = {
                    type: "query",
                    val: ""
                }
                var index = parentGroup.queryGroup.indexOf(query);
                parentGroup.queryGroup.splice(index + 1, 0, sibQuery);
                appendInnerSearchQuery(event.target.parentNode, level, sibQuery, parentGroup, false);
            });
            myDiv.appendChild(termbtn);

            // Add group to a group (button)
            var grpbtn = document.createElement("input");
            grpbtn.type = "button";
            grpbtn.value = "++";
            grpbtn.addEventListener("click", function(event) {
                var label = document.getElementById("formerror");
                label.innerHTML = "";

                var sibGroup = {
                    type: "group",
                    val: "any",
                    queryGroup: []
                }
                var index = parentGroup.queryGroup.indexOf(query);
                parentGroup.queryGroup.splice(index + 1, 0, sibGroup);
                appendInnerSearchQuery(event.target.parentNode, level, sibGroup, parentGroup, false);

            });
            myDiv.appendChild(grpbtn);
  		}

    } else {
        // Add query to a group (button)
        var termbtn = document.createElement("input");
        termbtn.type = "button";
        termbtn.value = "+";
        termbtn.addEventListener("click", function(event) {
            var label = document.getElementById("formerror");
            label.innerHTML = "";

            var sibQuery = {
                type: "query",
                val: ""
            }
            var index = parentGroup.queryGroup.indexOf(query);
            parentGroup.queryGroup.splice(index + 1, 0, sibQuery);
            appendInnerSearchQuery(event.target.parentNode, level, sibQuery, parentGroup, false);
        });
        myDiv.appendChild(termbtn);
  
      // Add group to a group (button)
      var grpbtn = document.createElement("input");
      grpbtn.type = "button";
      grpbtn.value = "++";
      grpbtn.addEventListener("click", function(event) {
          var label = document.getElementById("formerror");
          label.innerHTML = "";

          var sibGroup = {
              type: "group",
              val: "any",
              queryGroup: []
          }
          var index = parentGroup.queryGroup.indexOf(query);
          parentGroup.queryGroup.splice(index + 1, 0, sibGroup);
          appendInnerSearchQuery(event.target.parentNode, level, sibGroup, parentGroup, false);
      });
      myDiv.appendChild(grpbtn);

    }
}

// No parent argument, because this is the root
//appendInnerSearchQuery(document.querySelector('.wrapper'), 0, rootGroup);
