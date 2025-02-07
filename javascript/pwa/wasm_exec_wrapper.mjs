// wasm_exec_wrapper.mjs
import '../go_wasm/wasm_exec.js';  // Adjust the path as necessary.

// Export the global Go if it is available.
if (typeof Go === "undefined") {
  throw new Error("Go is not defined. Make sure wasm_exec.js executed correctly.");
}
export { Go };