class TTLCache {
  constructor(max=500){
    this.max = max;
    this.map = new Map();
  }
  set(key, value, ttlMs){
    const exp = Date.now() + (ttlMs || 0);
    this.map.set(key, { value, exp });
    if (this.map.size > this.max) {
      const first = this.map.keys().next().value;
      this.map.delete(first);
    }
  }
  get(key){
    const hit = this.map.get(key);
    if (!hit) return null;
    if (hit.exp && Date.now() > hit.exp) { this.map.delete(key); return null; }
    return hit.value;
  }
}
module.exports = { TTLCache };
