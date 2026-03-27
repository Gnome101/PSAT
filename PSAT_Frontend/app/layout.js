import "./globals.css";

export const metadata = {
  title: "Protocal Security Assessment Tool",
  description:
    "A simple platform to evaluate, report, and improve protocol security posture."
};

export default function RootLayout({ children }) {
  return (
    <html lang="en">
      <body>{children}</body>
    </html>
  );
}
