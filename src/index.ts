import express from "express"

const app = express()
const PORT = 4500


app.use(express.json());

app.get("/", (req, res) => {
  res.send("Hello World!")
})

app.use((_, res) => {
  res.status(404).json({ message: "Route not found" });
});

app.listen(PORT, () => {
  console.log(`Server running on ${PORT}`)
})