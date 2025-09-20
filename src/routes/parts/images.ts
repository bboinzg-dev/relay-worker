// GET /api/parts/:brand/:code/images
export async function listImages(req, res) {
  const { brand, code } = req.params;
  const { rows } = await pool.query(
    `SELECT gcs_uri FROM public.image_index
      WHERE brand_norm = lower($1) AND code_norm = lower($2)
      ORDER BY created_at DESC LIMIT 12`,
    [brand, code]
  );

  const images = await Promise.all(rows.map(async r => ({
    url: await signGcsUrl(r.gcs_uri)
  })));

  res.json({ images });
}
