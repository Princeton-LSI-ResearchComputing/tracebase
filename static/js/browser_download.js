/*
 * Send this function a filename and text and it will "create" a file with the given name and start a "download" of that
 * file (whose content came from the page).  To do this, it adds a hidden a tag to the DOM, "clicks" it to start the
 * download, then removes itself from the DOM.
 *
 * Example usage:
 *      <div id="data-download" style="display: none;">THIS IS THE CONTENT OF THE FILE</div>
 *      <a href="javascript:browserDownloadText('filename.txt', document.getElementById('data-download').innerHTML)">
 *          download
 *      </a>
 *
 * This code was based on the following article:
 * https://ourcodeworld.com/articles/read/189/how-to-create-a-file-and-generate-a-download-with-javascript-in-the-browser-without-a-server
 */
function browserDownloadText (filename, text) { // eslint-disable-line no-unused-vars
  const element = document.createElement('a');
  element.setAttribute('href', 'data:text/plain;base64,' + encodeURIComponent(text));
  element.setAttribute('download', filename);
  element.style.display = 'none';
  document.body.appendChild(element);
  element.click();
  document.body.removeChild(element);
}

function browserDownloadExcel (filename, base64Text) { // eslint-disable-line no-unused-vars
  const element = document.createElement('a');
  element.setAttribute(
    'href',
    'data:application/vnd.openxmlformats-officedocument.spreadsheetml.sheet;base64,' + encodeURIComponent(base64Text)
  );
  element.setAttribute('download', filename);
  element.style.display = 'none';
  document.body.appendChild(element);
  element.click();
  document.body.removeChild(element);
}

function browserDownloadBase64 (filename, content, type) { // eslint-disable-line no-unused-vars
  if (type === 'text') {
    browserDownloadText(filename, content);
  } else if (type === 'excel') {
    browserDownloadExcel(filename, content);
  } else {
    console.error("Download type invalid: " + type);
  }
}

function browserDownloadBase64 (filename, content, type) { // eslint-disable-line no-unused-vars
  if (type === 'text') {
    browserDownloadText(filename, content)
  } else if (type === 'excel') {
    browserDownloadExcel(filename, content)
  } else {
    console.error('Download type invalid: ' + type)
  }
}

function browserDownloadBase64 (filename, content, type) { // eslint-disable-line no-unused-vars
  if (type === 'text') {
    browserDownloadText(filename, content)
  } else if (type === 'excel') {
    browserDownloadExcel(filename, content)
  } else {
    console.error('Download type invalid: ' + type)
  }
}
