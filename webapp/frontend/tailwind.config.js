/** @type {import('tailwindcss').Config} */
export default {
  content: ["./index.html", "./src/**/*.{ts,tsx}"],
  theme: {
    extend: {
      fontFamily: {
        sans: ["Inter", "ui-sans-serif", "system-ui", "sans-serif"],
        mono: ["JetBrains Mono", "ui-monospace", "monospace"],
      },
      keyframes: {
        flow: { "0%": { backgroundPosition: "0 0" }, "100%": { backgroundPosition: "24px 0" } },
        "slide-in": { "0%": { opacity: "0", transform: "translateY(-6px)" }, "100%": { opacity: "1", transform: "none" } },
      },
      animation: {
        flow: "flow 0.8s linear infinite",
        "slide-in": "slide-in 0.3s ease-out",
      },
    },
  },
  plugins: [],
};
