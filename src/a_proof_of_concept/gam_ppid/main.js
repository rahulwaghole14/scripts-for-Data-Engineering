
  // Function to hash the PPID
  function hashPPID() {
    const account = `1999-01-01/x.x@hexa.co.nz`
    const urlDecoded = decodeURIComponent(account);
    const base64Decoded = CryptoJS.enc.Base64.parse(urlDecoded);
    return CryptoJS.SHA512(base64Decoded).toString(CryptoJS.enc.Hex)
  }

  // Include CryptoJS library for hashing
  const script = document.createElement('script');
  script.src = "https://cdnjs.cloudflare.com/ajax/libs/crypto-js/4.1.1/crypto-js.min.js";
  document.head.appendChild(script);

  // Test the function once CryptoJS is loaded
  script.onload = () => {
    // Example of setting cookies for testing
    console.log(hashPPID()); // Should output the hashed PPID
  };
