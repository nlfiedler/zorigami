import { defineConfig } from 'vite';
import solidPlugin from 'vite-plugin-solid';
import devtools from 'solid-devtools/vite';

export default defineConfig({
  plugins: [devtools(), solidPlugin()],
//   resolve: {
//     dedupe: ['solid-js']
//   },
  build: {
    target: 'esnext',
  },
});
