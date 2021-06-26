// Developing this in jsfiddle: http://jsfiddle.net/bqk6pmjg/

var rootGroup = {
    type: "group",
    val: "all",
    queryGroup: []
};
  
function appendInnerSearchQuery(element, query, copyQuery, parentGroup, afterMode) {
    "use strict";

    var undef;

    var isInit = false;
    if (typeof copyQuery !== 'undefined' || copyQuery) {
        isInit = true;
    }


    if (typeof afterMode === 'undefined') {
        afterMode = false;
    }
    console.log("Appending after sibling? ", afterMode);

    var isRoot = true;
    if (typeof parentGroup !== 'undefined' || parentGroup) {
        isRoot = false;
    }

    var myDiv = document.createElement("div");
    if (!isRoot) {
        myDiv.className = "level-indent"
    }

    var isGroup = false;
  
    if (('' + query.type) === 'group') {
  
        isGroup = true;

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
            if (isInit) {
                var keyname = clones[i].name.split("-").pop();
                query[keyname] = copyQuery[keyname];
                clones[i].value = copyQuery[keyname];
                console.log("Converted long key name ",clones[i].name," to ",keyname," to obtain previous value: ",copyQuery[keyname]," and produced DOM element: ",clones[i]," with value: ",clones[i].value);
            } else {
                query[clones[i].name] = clones[i].value;
            }
            
            // Add this row to the HTML form
            myDiv.appendChild(clones[i]);
        }

        console.log("Added div leaf with data: ", myDiv);
        
    } else {
        var label = document.getElementById("formerror");
        label.innerHTML = "Error: Unrecognized query type: " + query.type;
    }
  
    if (!isRoot) {
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
    
    if (afterMode) {
        element.after(myDiv);
    } else {
	    element.appendChild(myDiv);
    }
  
  
    if (isGroup) {

        if (!isInit) {
            // Add a couple queries to start off
            var subQuery = {
                type: "query",
                val: ""
            }
            query.queryGroup.push(subQuery);
            appendInnerSearchQuery(myDiv, subQuery, undef, query);

            // If this isn't the root, append a second query form
            if (!isRoot) {
                var subQuery2 = {
                    type: "query",
                    val: ""
                }
                query.queryGroup.push(subQuery2);
                appendInnerSearchQuery(myDiv, subQuery2, undef, query);
            }
        }
        
        myDiv.append(document.createElement("div"));

        if (!isRoot && !isInit) {

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
                appendInnerSearchQuery(event.target.parentNode, sibQuery, undef, parentGroup, true);
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
                appendInnerSearchQuery(event.target.parentNode, sibGroup, undef, parentGroup, true);

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
            appendInnerSearchQuery(event.target.parentNode, sibQuery, undef, parentGroup, true);
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
          appendInnerSearchQuery(event.target.parentNode, sibGroup, undef, parentGroup, true);
      });
      myDiv.appendChild(grpbtn);
    }
    console.log("Updated data structure: ",rootGroup);

    // Return the div that was created
    return myDiv;
}

function initializeExistingSearchQuery(element, initQuery) {
    console.log("Initial query: ", initQuery);
    console.log("Root group: ", rootGroup);

    // Create the root object
    var childDiv = appendInnerSearchQuery(element, rootGroup, initQuery[0]);
    initializeExistingSearchQueryHelper(childDiv, initQuery[0].queryGroup, rootGroup);
}

function initializeExistingSearchQueryHelper(element, copyQueryArray, parentNode) {
    var undef;

    for (let i = 0; i < copyQueryArray.length; i++) {

        if (copyQueryArray[i].type === "group") {
            var subGroup = {
                type: "group",
                val: copyQueryArray[i].val,
                queryGroup: []
            };
            parentNode.queryGroup.push(subGroup);
            var childDiv = appendInnerSearchQuery(element, subGroup, copyQueryArray[i], parentNode, false);
            // Recurse
            initializeExistingSearchQueryHelper(childDiv, copyQueryArray[i].queryGroup, subGroup);

            ///////////////////// I NEED TO FIGURE OUT HOW TO APPEND + AND ++ BUTTONS HERE

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
                var index = parentNode.queryGroup.indexOf(subGroup);
                parentNode.queryGroup.splice(index + 1, 0, sibQuery);
                appendInnerSearchQuery(event.target.parentNode, sibQuery, undef, parentNode, true);
            });
            childDiv.appendChild(termbtn);
            
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
                var index = parentNode.queryGroup.indexOf(subGroup);
                parentNode.queryGroup.splice(index + 1, 0, sibGroup);
                appendInnerSearchQuery(event.target.parentNode, sibGroup, undef, parentNode, true);
            });
            childDiv.appendChild(grpbtn);

        } else if(copyQueryArray[i].type === "query") {
            var subQuery = {
                type: "query",
            };
            parentNode.queryGroup.push(subQuery);
            appendInnerSearchQuery(element, subQuery, copyQueryArray[i], parentNode, false);

        } else {
            console.error("Unknown node type: ", copyQueryArray[i].type);
        }
    }
}

function saveSearchQueryHierarchy(divElem) {
    "use strict";

    //var childElems = divElem.children; // - Doesn't work
    //var childElems = divElem.querySelectorAll(":scope > input,select,textarea,label,div");
    var childDivs = divElem.querySelectorAll(":scope > div"); // - results in only 1, even if 2 items added - I think because each input is not wrapped in a div

    var total = 0;

    // This should only traverse a single iteration (because there's only one root)
    for (let i = 0; i < childDivs.length; i++) {
        //console.log("Child " + i + " of " + divElem.name + " with name " + childElems[i].name + " of type " + childElems[i].type);
        //console.log("Child " + i + " of " + divElem.name + " at index " + 0 + ":",childElems[i]);

        total = saveSearchQueryHierarchyHelper(childDivs[i], "", 0, 0);
        // Add this row to the HTML form
        //myDiv.appendChild(clones[i]);
    }

    console.log("New Div Structure: ",divElem);
    var formInput = document.getElementById("id_form-TOTAL_FORMS");
    formInput.value = total;
    console.log("Setting total " + total + " for id_form-TOTAL_FORMS: ",formInput);
}

function saveSearchQueryHierarchyHelper(divElem, path, count, idx) {
    "use strict";

    console.log("Looking at: ", divElem);

    //var childElems = divElem.children; // - Doesn't work
    //var childElems = divElem.querySelectorAll(":scope > input,select,textarea,label,div");
    var childDivs = divElem.querySelectorAll(":scope > div"); // - results in only 1, even if 2 items added - I think because each input is not wrapped in a div

    var numChildren = (childDivs.length - 1);
    if (numChildren > -1) {
        console.log("Num children: " + numChildren + ":");
    }

    if ( path === "") {
        path += idx;
    } else {
        path += "." + idx;
    }

    //var childInputs = divElem.querySelectorAll("input,select,textarea");
    // This gets inputs belonging to the parent
    var childInputs = divElem.childNodes;

    let isForm = false;
    let isAll = true;
    for (let i = 0; i < childInputs.length; i++) {
        if (typeof childInputs[i].name !== 'undefined' && childInputs[i].name) {
            if (childInputs[i].name.includes("-pos")) {
                childInputs[i].value = path;
                isForm = true;
                count++;
                console.log("Found form.  Incremented count to: " + count);
            } else if (childInputs[i].name.includes("grouptype") && childInputs[i].value === "any") {
                isAll = false;
            }
            console.log("  Child " + childInputs[i].name + " of type " + childInputs[i].type + " with value: " + childInputs[i].value);
        }
    }

    console.log("isForm: ", isForm);

    // If this is a form from Django formset form (otherwise it's a hierarchy control level)
    if (isForm) {
        for (let i = 0; i < childInputs.length; i++) {
            if (typeof childInputs[i].name !== 'undefined' && childInputs[i].name) {
                console.log("  Old attributes: ", childInputs[i]);
                // Replace (e.g. "form-0-val") with "form-<count>-val"
                let re = /-0-|-__prefix__-/;
                let replacement = '-' + (count - 1) + '-';
                console.log("Replacing -0- with " + replacement);
                if (childInputs[i].for) childInputs[i].for = childInputs[i].for.replace(re, replacement);
                if (childInputs[i].id) {
                    let tmp = childInputs[i].id;
                    let newid = tmp.replace(re, replacement);
                    childInputs[i].id = newid;
                }
                if (childInputs[i].name) childInputs[i].name = childInputs[i].name.replace(re, replacement);
                console.log("  New attributes: ", childInputs[i]);
            }
        }
    } else {
        if (isAll) {
            path += "-all";
        } else {
            path += "-any";
        }
    }

    let total = 0;

    // Recurse
    // Always traverse 1 less, because there's always an empty trailing div tag
    for (let i = 0; i < (childDivs.length - 1); i++) {
        console.log("Recursing to child " + i + ": ", childDivs[i]);
        //console.log("Child " + i + " of " + divElem.name + " at index " + idx + ":",childElems[i]);

        count = saveSearchQueryHierarchyHelper(childDivs[i], path, count, i);
        // Add this row to the HTML form
        //myDiv.appendChild(clones[i]);
    }

    return count;
}

// No parent argument, because this is the root
//appendInnerSearchQuery(document.querySelector('.wrapper'), rootGroup);
