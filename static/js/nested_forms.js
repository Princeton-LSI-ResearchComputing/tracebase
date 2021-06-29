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

        // Initialize the value in the hierarchy with the default
        if (isInit) {
            query.val = copyQuery.val;
        }
        
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
    	var elements = templateDiv.querySelectorAll("input,select,textarea");
        let clones = [];
        elements.forEach(function(elem) {
            clones.push(elem.cloneNode(true));
        });
    	
        var errors = [];
        // For each clones input form element
        for (let i = 0; i < clones.length; i++) {
        	
            // Dismiss any previous error (that was previously presented and prevented)
            clones[i].addEventListener("click", function(event) {
                var label = document.getElementById("formerror");
                label.innerHTML = "";
                console.log("Click field 1");
            });
            
            // Keep the value of the hierarchy structure up to date when the user changes the form value
            clones[i].addEventListener("change", function() {
                query[clones[i].name] = event.target.value;
            });
            
            // Initialize the value in the hierarchy with the default
            if (isInit) {
                var keyname = clones[i].name.split("-").pop();
                query[keyname] = copyQuery[keyname];
                clones[i].value = copyQuery[keyname];
                console.log("Converted long key name ",clones[i].name," to ",keyname," to obtain previous value: ",copyQuery[keyname]," and produced DOM element: ",clones[i]," with value: ",clones[i].value);

                // If this isn't the hidden pos field and there is no value, push an error
                if (keyname !== "pos" && copyQuery[keyname] === "") {
                    errors.push(" * This is a required field.");
                }
            } else {
                query[clones[i].name] = clones[i].value;
            }
            
            // Add this row to the HTML form
            myDiv.appendChild(clones[i]);

            // If there were any errors, create an error label
            // For some reason, this was a nice tooltip in an earlier version (f9c2cac151f9909380022cea8b7a40a5f0e72a4e), but doesn't work automatically in the latest version
            if (errors.length > 0) {
                var errlabel = document.createElement("label");
                errlabel.className = "text-danger";
                errlabel.innerHTML = "";
                for (let j = 0; j < errors.length; j++) {
                    errlabel.innerHTML += errors[j] + " ";
                }
                myDiv.appendChild(errlabel);
            }
        }

        console.log("Added div leaf with data: ", myDiv);
        
    } else {
        var label = document.getElementById("formerror");
        label.innerHTML = "Error: Unrecognized query type: " + query.type;
    }
  
    if (!isRoot) {
        var rmBtn = document.createElement("a");
        rmBtn.href = "javascript:void(0)";
        var btnImg = document.createElement("img");
        btnImg.src = "/static/images/minus.png";
        rmBtn.appendChild(btnImg);
        rmBtn.addEventListener("click", function(event) {
            var label = document.getElementById("formerror");
            label.innerHTML = "";
            console.log("Click field 2");

            var size = parentGroup.queryGroup.length;
            if (size <= 1) {
                var label = document.getElementById("formerror");
                label.innerHTML = "A match group must have at least 1 query.";
            } else {
                event.target.parentNode.parentNode.remove();
                var index = parentGroup.queryGroup.indexOf(query);
                parentGroup.queryGroup.splice(index, 1);
            }
        });
        myDiv.appendChild(document.createTextNode(" "));
        myDiv.appendChild(rmBtn);
    }
    
    if (afterMode) {
        element.after(myDiv);
    } else {
	    element.appendChild(myDiv);
    }
  
  
    if (isGroup) {

        // Initialization using a copied rootgroup adds items one at a time, so don't pre-add empties.
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

            // Not exactly sure why, but after adding inner elements to a group, an empty div is needed to make future dynamically-added form elements to be correctly created.  I did this based on the template post I followed that had a static empty div just inside where the dynamic content was being created, when stuff I was adding wasn't working right and it seems to have fixed it.
            myDiv.append(document.createElement("div"));
        }
        
        // Initialization using a copied rootgroup adds items one at a time, so don't add the follow-up + and ++ buttons.  This way, the individually eppended inner forms don't go under these buttons.  This means that the initializing function must add these manually.
        if (!isRoot && !isInit) {

            // Add query to a group (button)
            var termbtn = document.createElement("a");
            termbtn.href = "javascript:void(0)";
            var btnImg = document.createElement("img");
            btnImg.src = "/static/images/plus.png";
            termbtn.appendChild(btnImg);
            termbtn.addEventListener("click", function(event) {
                var label = document.getElementById("formerror");
                label.innerHTML = "";
                console.log("Click field 3");

                var sibQuery = {
                    type: "query",
                    val: ""
                }
                var index = parentGroup.queryGroup.indexOf(query);
                parentGroup.queryGroup.splice(index + 1, 0, sibQuery);
                // The clicked item is the image, so to get the eclosing div, we need the grandparent
                appendInnerSearchQuery(event.target.parentNode.parentNode, sibQuery, undef, parentGroup, true);
            });
            myDiv.appendChild(document.createTextNode(" "));
            myDiv.appendChild(termbtn);

            // Add group to a group (button)
            var grpbtn = document.createElement("a");
            grpbtn.href = "javascript:void(0)";
            var btnImg = document.createElement("img");
            btnImg.src = "/static/images/plusplus.png";
            grpbtn.appendChild(btnImg);
            grpbtn.addEventListener("click", function(event) {
                var label = document.getElementById("formerror");
                label.innerHTML = "";
                console.log("Click field 4");

                var sibGroup = {
                    type: "group",
                    val: "any",
                    queryGroup: []
                }
                var index = parentGroup.queryGroup.indexOf(query);
                parentGroup.queryGroup.splice(index + 1, 0, sibGroup);
                // The clicked item is the image, so to get the eclosing div, we need the grandparent
                appendInnerSearchQuery(event.target.parentNode.parentNode, sibGroup, undef, parentGroup, true);

            });
            myDiv.appendChild(document.createTextNode(" "));
            myDiv.appendChild(grpbtn);
  		}

    } else {
        // Add query to a group (button)
        var termbtn = document.createElement("a");
        termbtn.href = "javascript:void(0)";
        var btnImg = document.createElement("img");
        btnImg.src = "/static/images/plus.png";
        termbtn.appendChild(btnImg);
        termbtn.addEventListener("click", function(event) {
            var label = document.getElementById("formerror");
            label.innerHTML = "";
            console.log("Click field 5");

            var sibQuery = {
                type: "query",
                val: ""
            }
            var index = parentGroup.queryGroup.indexOf(query);
            parentGroup.queryGroup.splice(index + 1, 0, sibQuery);
            // The clicked item is the image, so to get the eclosing div, we need the grandparent
            appendInnerSearchQuery(event.target.parentNode.parentNode, sibQuery, undef, parentGroup, true);
        });
        myDiv.appendChild(document.createTextNode(" "));
        myDiv.appendChild(termbtn);
  
        // Add group to a group (button)
        var grpbtn = document.createElement("a");
        grpbtn.href = "javascript:void(0)";
        var btnImg = document.createElement("img");
        btnImg.src = "/static/images/plusplus.png";
        grpbtn.appendChild(btnImg);
        grpbtn.addEventListener("click", function(event) {
            var label = document.getElementById("formerror");
            label.innerHTML = "";
            console.log("Click field 6");

            var sibGroup = {
                type: "group",
                val: "any",
                queryGroup: []
            }
            var index = parentGroup.queryGroup.indexOf(query);
            parentGroup.queryGroup.splice(index + 1, 0, sibGroup);
            // The clicked item is the image, so to get the eclosing div, we need the grandparent
            appendInnerSearchQuery(event.target.parentNode.parentNode, sibGroup, undef, parentGroup, true);
        });
        myDiv.appendChild(document.createTextNode(" "));
        myDiv.appendChild(grpbtn);
    }
    console.log("Updated data structure: ",rootGroup);

    // Return the div that was created
    return myDiv;
}

function initializeExistingSearchQuery(element, initQuery) {
    "use strict";

    console.log("Initial query: ", initQuery);
    console.log("Root group: ", rootGroup);

    // Create the root object
    var childDiv = appendInnerSearchQuery(element, rootGroup, initQuery[0]);

    initializeExistingSearchQueryHelper(childDiv, initQuery[0].queryGroup, rootGroup);

    // Not exactly sure why, but after adding inner elements to a group, an empty div is needed to make future dynamically-added form elements to be correctly created.  I did this based on the template post I followed that had a static empty div just inside where the dynamic content was being created, when stuff I was adding wasn't working right and it seems to have fixed it.
    childDiv.append(document.createElement("div"));
}

function initializeExistingSearchQueryHelper(element, copyQueryArray, parentNode) {
    "use strict";

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

            // Not exactly sure why, but after adding inner elements to a group, an empty div is needed to make future dynamically-added form elements to be correctly created.  I did this based on the template post I followed that had a static empty div just inside where the dynamic content was being created, when stuff I was adding wasn't working right and it seems to have fixed it.
            childDiv.append(document.createElement("div"));

            // Add query to a group (button)
            var termbtn = document.createElement("a");
            termbtn.href = "javascript:void(0)";
            var btnImg = document.createElement("img");
            btnImg.src = "/static/images/plus.png";
            termbtn.appendChild(btnImg);
            termbtn.addEventListener("click", function(event) {
                var label = document.getElementById("formerror");
                label.innerHTML = "";
                console.log("Click field 7");

                var sibQuery = {
                    type: "query",
                    val: ""
                }
                var index = parentNode.queryGroup.indexOf(subGroup);
                parentNode.queryGroup.splice(index + 1, 0, sibQuery);
                // The clicked item is the image, so to get the eclosing div, we need the grandparent
                appendInnerSearchQuery(event.target.parentNode.parentNode, sibQuery, undef, parentNode, true);
            });
            childDiv.appendChild(document.createTextNode(" "));
            childDiv.appendChild(termbtn);
            
            // Add group to a group (button)
            var grpbtn = document.createElement("a");
            grpbtn.href = "javascript:void(0)";
            var btnImg = document.createElement("img");
            btnImg.src = "/static/images/plusplus.png";
            grpbtn.appendChild(btnImg);
            grpbtn.addEventListener("click", function(event) {
                var label = document.getElementById("formerror");
                label.innerHTML = "";
                console.log("Click field 8");

                var sibGroup = {
                    type: "group",
                    val: "any",
                    queryGroup: []
                }
                var index = parentNode.queryGroup.indexOf(subGroup);
                parentNode.queryGroup.splice(index + 1, 0, sibGroup);
                // The clicked item is the image, so to get the eclosing div, we need the grandparent
                appendInnerSearchQuery(event.target.parentNode.parentNode, sibGroup, undef, parentNode, true);
            });
            childDiv.appendChild(document.createTextNode(" "));
            childDiv.appendChild(grpbtn);

        } else if(copyQueryArray[i].type === "query") {
            var subQuery = {
                type: "query",
            };
            parentNode.queryGroup.push(subQuery);
            appendInnerSearchQuery(element, subQuery, copyQueryArray[i], parentNode, false);

        } else {
            console.error("Unknown node type at index " + i + ": ", copyQueryArray[i].type);
        }
    }
}

function saveSearchQueryHierarchy(divElem) {
    "use strict";

    //var childElems = divElem.querySelectorAll(":scope > input,select,textarea,label,div");
    var childDivs = divElem.querySelectorAll(":scope > div"); // - results in only 1, even if 2 items added - I think because each input is not wrapped in a div

    var total = 0;

    // This should only traverse a single iteration (because there's only one root)
    for (let i = 0; i < childDivs.length; i++) {
        //console.log("Child " + i + " of " + divElem.name + " with name " + childElems[i].name + " of type " + childElems[i].type);
        //console.log("Child " + i + " of " + divElem.name + " at index " + 0 + ":",childElems[i]);

        total = saveSearchQueryHierarchyHelper(childDivs[i], "", 0, 0);
    }

    console.log("New Div Structure: ",divElem);
    var formInput = document.getElementById("id_form-TOTAL_FORMS");
    formInput.value = total;
    console.log("Setting total " + total + " for id_form-TOTAL_FORMS: ",formInput);
}

function saveSearchQueryHierarchyHelper(divElem, path, count, idx) {
    "use strict";

    console.log("Looking at: ", divElem);

    //var childElems = divElem.querySelectorAll(":scope > input,select,textarea,label,div");
    var childDivs = divElem.querySelectorAll(":scope > div"); // - results in only 1, even if 2 items added - I think because each input is not wrapped in a div

    // Always traverse 1 less, because there's always an empty trailing div tag
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
    for (let i = 0; i < numChildren; i++) {
        console.log("Recursing to child " + i + ": ", childDivs[i]);

        count = saveSearchQueryHierarchyHelper(childDivs[i], path, count, i);
    }

    return count;
}
