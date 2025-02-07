// // index.mjs
// import { readFile } from 'fs/promises';
// import { fileURLToPath } from 'url';

// (async () => {
//   // Load wasm_exec.js so that the global `Go` class is defined.
//   await import('./go_wasm/wasm_exec.js');

//   // Ensure that wasm_exec.js has defined the global Go.
//   if (typeof Go === "undefined") {
//     throw new Error("Go is not defined. Check if wasm_exec.js loaded correctly.");
//   }

//   const go = new Go();

//   // Convert the relative URL to an absolute file URL,
//   // then convert that URL to a local file system path.
//   const wasmFileUrl = new URL('./go_wasm/gophers.wasm', import.meta.url);
//   const wasmFilePath = fileURLToPath(wasmFileUrl);

//   // Read the WASM file as a buffer.
//   const wasmBuffer = await readFile(wasmFilePath);

//   // Instantiate the WebAssembly module from the buffer.
//   const result = await WebAssembly.instantiate(wasmBuffer, go.importObject);

//   // Run the Go WebAssembly module.
//   go.run(result.instance);
// })();

// index.mjs
// This function initializes the Go WASM module and returns an object with exported functions.
export async function loadGoWasm() {
  // Dynamically import wasm_exec.js. (Make sure it’s included in your package.)
  await import('./go_wasm/wasm_exec.js');

  // Create a new Go instance.
  const go = new Go();

  // Construct an absolute URL for the WASM file relative to this module.
  const wasmURL = new URL('./go_wasm/gophers.wasm', import.meta.url);

  // Use instantiateStreaming with a fallback to ArrayBuffer.
  let result;
  try {
    result = await WebAssembly.instantiateStreaming(fetch(wasmURL), go.importObject);
  } catch (streamingError) {
    console.warn("instantiateStreaming failed, falling back:", streamingError);
    const response = await fetch(wasmURL);
    const buffer = await response.arrayBuffer();
    result = await WebAssembly.instantiate(buffer, go.importObject);
  }

  // Run the Go WebAssembly module. Note that go.run is asynchronous,
  // but it blocks further execution until the Go code stops.
  // In our case, the Go code never exits (because of select{}), but that’s fine.
  go.run(result.instance);

  // At this point, the Go code has registered its functions on the global object.
  // Return an object with references to the exported functions.
  return {
    add: globalThis.add
    // Add other exported functions here if needed.
  };
}
