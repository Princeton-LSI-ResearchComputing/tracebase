// A dictionary of dropArea info, e.g. dropAreas["dropAreakKey"] = {
//     "dropArea": dropArea,
//     "fileFunc": fileFunc,
//     "postDropFunc": postDropFunc,
//     "filesList": filesList
// }
var dropAreas = {} // eslint-disable-line no-unused-vars

// This code is based on the following article:
// https://www.smashingmagazine.com/2018/01/drag-drop-file-uploader-vanilla-js/

/**
 * This initializes all of the global variables.
 * @param {*} dropArea is the div element where files are dropped
 * @param {*} fileFunc is a function that takes DataTransfer object containing a single file.  It will be called for
 *   every dropped file.
 * @param {*} postDropFunc is an optional function without arguments that is called after all the files have been
 *   processed.
 */
function initDropArea (dropArea, fileFunc, postDropFunc) { // eslint-disable-line no-unused-vars
  ;['dragenter', 'dragover', 'dragleave', 'drop'].forEach(eventName => {
    dropArea.addEventListener(eventName, preventDefaults, false)
  })

  ;['dragenter', 'dragover'].forEach(eventName => {
    dropArea.addEventListener(eventName, highlight, false,)
  })

  ;['dragleave', 'drop'].forEach(eventName => {
    dropArea.addEventListener(eventName, unhighlight, false)
  })

  let dropAreaKey = dropArea.id;

  globalThis.dropAreas[dropAreaKey] = {
    "dropArea": dropArea,
    "fileFunc": null,
    "postDropFunc": null,
    "filesList": []
  }

  if (typeof fileFunc !== 'undefined' && fileFunc) {
    globalThis.dropAreas[dropAreaKey]["fileFunc"] = fileFunc
  }
  
  if (typeof postDropFunc !== 'undefined' && postDropFunc) {
    globalThis.dropAreas[dropAreaKey]["postDropFunc"] = postDropFunc
  }

  // dropArea.addEventListener('drop', function (e) {handleDrop(e, dropAreaKey)}, false)
  console.log("Attaching drop area listener to:", dropArea)
  dropArea.addEventListener('drop', onDropItems, false)
}

function preventDefaults (e) { // eslint-disable-line no-unused-vars
  e.preventDefault()
  e.stopPropagation()
}

function highlight (e) {
  this.classList.add('highlight')
}

function unhighlight (e) {
  this.classList.remove('highlight')
}

function handleDrop (event, dropAreaKey) {
  console.log("Trying this out")
  // See: https://web.dev/patterns/files/drag-and-drop-directories#js
  const fileHandlesPromises = [...event.dataTransfer.items]
    .filter((item) => item.kind === "file")
    .map((item) =>
      supportsFileSystemAccessAPI
        ? item.getAsFileSystemHandle()
        : supportsWebkitGetAsEntry
        ? item.webkitGetAsEntry()
        : item.getAsFile()
    );

  console.log("promises:", fileHandlesPromises)

  for (const handle of fileHandlesPromises) {
    if (handle.kind === "directory" || handle.isDirectory) {
      console.log(`Directory: ${handle.name}`);
      console.log("handle:", handle)
      
      // See: https://udn.realityripple.com/docs/Web/API/FileSystemDirectoryReader/readEntries
      // let directoryReader = handle.createReader();
      // console.log("directoryReader:", directoryReader)
      // files = directoryReader.readEntries(function(entries) {
      //     entries.forEach(function(entry) {
      //       scanFiles(entry, directoryContainer);
      //   });
      // });
      let files = [];
      scanFiles(handle, files);
      console.log("files from reading dir:", files)
      // debug.textContent += `Directory: ${handle.name}\n`;
    } else {
      console.log(`File: ${handle.name}`);
      // debug.textContent += `File: ${handle.name}\n`;
    }
  }

  // for await (const handle of fileHandlesPromises) {
  //   if (handle.kind === "directory" || handle.isDirectory) {
  //     console.log(`Directory: ${handle.name}`);
  //     // debug.textContent += `Directory: ${handle.name}\n`;
  //   } else {
  //     console.log(`File: ${handle.name}`);
  //     // debug.textContent += `File: ${handle.name}\n`;
  //   }
  // }


  const dt = event.dataTransfer
  const files = dt.files
  handleFiles(files, dropAreaKey)
}

// See: https://udn.realityripple.com/docs/Web/API/FileSystemDirectoryReader/readEntries
function scanFiles(item, container) {
  if (item.isDirectory) {
    console.log("directory:", item.fullPath)
    let directoryReader = item.createReader();
    directoryReader.readEntries(function(entries) {
      entries.forEach(function(entry) {
        scanFiles(entry, container);
      });
    });
  } else {
    console.log("file:", item.fullPath)
    container.push(item)
  }
}

const supportsFileSystemAccessAPI = "getAsFileSystemHandle" in DataTransferItem.prototype;
const supportsWebkitGetAsEntry = "webkitGetAsEntry" in DataTransferItem.prototype;

// See: https://mikeyland.netlify.app/post/multi-file-upload-made-easy-how-to-drag-and-drop-directories-in-your-web-app
// This is an experiment to see if I can handle both drag and drop directories and input file directory selection
const onDropItems = async (e) => {
  // Prevent navigation.
  e.preventDefault();

  // Check for file system access capabilities
  if (!supportsFileSystemAccessAPI && !supportsWebkitGetAsEntry) {
    // Cannot handle directories.
    return;
  }

  const files = await getAllFileEntries(e.dataTransfer.items);
  const flattenFiles = files.reduce((acc, val) => acc.concat(val), []);
  console.log("Results here dude!!! : ", flattenFiles);
  console.log("Here is 'e':", e)
  // Added "this" to refer to the drop-area.  We use its id as a key to the dropAreas data structure.
  setResults(flattenFiles, e.target.parentElement);
};

// See: https://mikeyland.netlify.app/post/multi-file-upload-made-easy-how-to-drag-and-drop-directories-in-your-web-app
// Supports onDropItems
const getAllFileEntries = async (dataTransferItemList) => {
  let fileEntries = [];
  // Use BFS to traverse entire directory/file structure
  let queue = [];
  // Unfortunately dataTransferItemList is not iterable i.e. no forEach
  for (let i = 0; i < dataTransferItemList.length; i++) {
    queue.push(dataTransferItemList[i].webkitGetAsEntry());
  }
  while (queue.length > 0) {
    let entry = queue.shift();
    if (entry.isFile) {
      fileEntries.push(entry);
    } else if (entry.isDirectory) {
      let reader = entry.createReader();
      queue.push(...(await readAllDirectoryEntries(reader)));
    }
  }
  // return fileEntries;
  return Promise.all(
    fileEntries.map((entry) => readEntryContentAsync(entry))
  );
};

// See: https://mikeyland.netlify.app/post/multi-file-upload-made-easy-how-to-drag-and-drop-directories-in-your-web-app
// Supports onDropItems
// Get all the entries (files or sub-directories) in a directory by calling readEntries until it returns empty array
const readAllDirectoryEntries = async (directoryReader) => {
  let entries = [];
  let readEntries = await readEntriesPromise(directoryReader);
  while (readEntries.length > 0) {
    entries.push(...readEntries);
    readEntries = await readEntriesPromise(directoryReader);
  }
  return entries;
};

// See: https://mikeyland.netlify.app/post/multi-file-upload-made-easy-how-to-drag-and-drop-directories-in-your-web-app
// Supports onDropItems
// Wrap readEntries in a promise to make working with readEntries easier
const readEntriesPromise = async (directoryReader) => {
  try {
    return await new Promise((resolve, reject) => {
      directoryReader.readEntries(resolve, reject);
    });
  } catch (err) {
    console.error(err);
  }
};

// See: https://mikeyland.netlify.app/post/multi-file-upload-made-easy-how-to-drag-and-drop-directories-in-your-web-app
// Supports onDropItems
const readEntryContentAsync = async (entry) => {
  return new Promise((resolve, reject) => {
    let reading = 0;
    const contents = [];

    reading++;
    entry.file(async (file) => {
      reading--;
      const rawFile = file;
      rawFile.path = entry.fullPath;
      contents.push(rawFile);

      if (reading === 0) {
        resolve(contents);
      }
    });
  });
};

// See: https://mikeyland.netlify.app/post/multi-file-upload-made-easy-how-to-drag-and-drop-directories-in-your-web-app
// Supports onDropItems
function setResults(files, dropArea) {
  console.log("setResults dropArea:", dropArea)
  handleFiles(files, dropArea.id);
}

/**
 * This method calls the fileFunc that was initialized by the initDropArea function on each file and then calls the
 * postDropFunc, passing all files.
 * @param {*} files - An optional array of file objects.
 */
function handleFiles (files, dropAreaKey) { // eslint-disable-line no-unused-vars
  console.log("key:", dropAreaKey, "files", files, "dropAreas[dropAreaKey]:", dropAreas[dropAreaKey]);
  fileFunc = dropAreas[dropAreaKey]["fileFunc"];
  postDropFunc = dropAreas[dropAreaKey]["postDropFunc"];
  if (typeof files !== 'undefined' && files) {
    for (let i = 0; i < files.length; ++i) {
      // See: https://stackoverflow.com/questions/8006715/
      const dT = new DataTransfer() // eslint-disable-line no-undef
      dT.items.add(files[i])
      if (typeof fileFunc !== 'undefined' && fileFunc) {
        fileFunc(dT)
      }
    }
    if (typeof postDropFunc !== 'undefined' && postDropFunc) {
      postDropFunc(files)
    }
  }
}
