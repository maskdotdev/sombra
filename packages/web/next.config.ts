import type { NextConfig } from "next";

const nextConfig: NextConfig = {
  /* config options here */
  output: 'standalone',
  // Externalize native modules so they're not bundled by webpack/turbopack
  serverExternalPackages: ['@unyth/sombra'],
  
  // Ensure native modules work with webpack
  webpack: (config, { isServer }) => {
    if (isServer) {
      config.externals = config.externals || [];
      config.externals.push('@unyth/sombra');
    }
    return config;
  },
  
  turbopack: {
    root: "./",
  }
};

export default nextConfig;
