/** @type {import('next').NextConfig} */
const nextConfig = {
  // Export statique : aucun serveur requis. Les JSON de web/public/data sont
  // incorpores au build, donc le site reste consultable meme cluster eteint.
  output: "export",
  images: { unoptimized: true },
  reactStrictMode: true,
};
export default nextConfig;
