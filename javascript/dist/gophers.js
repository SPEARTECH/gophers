// gophers.js
// This function initializes the Go WASM module and returns an object with exported functions.
export async function Gophers() {
  // Dynamically import wasm_exec.js. (Make sure it’s included in your package.)
  await import('./static/go_wasm/wasm_exec.js');

  // Create a new Go instance.
  const go = new Go();

  // Construct an absolute URL for the WASM file relative to this module.
  const wasmURL = new URL('./static/go_wasm/gophers_js.wasm', import.meta.url);

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

  // Wait until Go sets globalThis.gophers
  await new Promise((resolve) => {
    const check = () => {
      if (globalThis.gophers) return resolve();
      setTimeout(check, 0);
    };
    check();
  });

  // Expose the WASM API and keep the namespace if you want direct access.
  // return {
  //   namespace: globalThis.gophers,
  //   ReadJSON: (...args) => globalThis.gophers.ReadJSON(...args), // returns JS DataFrame object with toJSON/toJSONFile/toCSVFile/free
  //   ReadCSV:  (...args) => globalThis.gophers.ReadCSV(...args),
  //   GetAPI:   (...args) => globalThis.gophers.GetAPI(...args),
  //   Free:     (...args) => globalThis.gophers.Free(...args),
  // };
  return globalThis.gophers;
}
