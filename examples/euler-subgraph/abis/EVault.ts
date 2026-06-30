import { parseAbiItem } from "abitype";

// EVault (Euler V2 Vault Kit) — the events handled by the subgraph's euler-vault template,
// plus the read functions its mappings call via .bind()/.try_* (→ Ponder readContract).
export const EVaultAbi = [
  // events
  parseAbiItem("event Deposit(address indexed sender, address indexed owner, uint256 assets, uint256 shares)"),
  parseAbiItem("event Withdraw(address indexed sender, address indexed receiver, address indexed owner, uint256 assets, uint256 shares)"),
  parseAbiItem("event Borrow(address indexed account, uint256 assets)"),
  parseAbiItem("event Repay(address indexed account, uint256 assets)"),
  parseAbiItem("event Liquidate(address indexed liquidator, address indexed violator, address collateral, uint256 repayAssets, uint256 yieldBalance)"),
  parseAbiItem("event Transfer(address indexed from, address indexed to, uint256 value)"),
  parseAbiItem("event VaultStatus(uint256 totalShares, uint256 totalBorrows, uint256 accumulatedFees, uint256 cash, uint256 interestAccumulator, uint256 interestRate, uint256 timestamp)"),
  // read functions (the subgraph's loadOrCreateEulerVault eth_call fan-out)
  parseAbiItem("function asset() view returns (address)"),
  parseAbiItem("function name() view returns (string)"),
  parseAbiItem("function symbol() view returns (string)"),
  parseAbiItem("function decimals() view returns (uint8)"),
  parseAbiItem("function oracle() view returns (address)"),
  parseAbiItem("function creator() view returns (address)"),
  parseAbiItem("function EVC() view returns (address)"),
] as const;
