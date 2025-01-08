package eth

// bad naming
import (
	"math/big"

	"github.com/usecorn/common-lib/conversions"
)

func ToDecimalForm(balance *big.Int, decimals int) *big.Float {
	pow := big.NewInt(10)
	pow = pow.Exp(pow, big.NewInt(int64(decimals)), nil)

	normalizedBalance := conversions.NewLargeFloat().SetInt(balance)
	normalizedBalance = normalizedBalance.Quo(normalizedBalance, conversions.NewLargeFloat().SetInt(pow))

	return normalizedBalance
}

func FromDecimalForm(balance *big.Float, decimals int) *big.Int {
	pow := big.NewInt(10)
	pow = pow.Exp(pow, big.NewInt(int64(decimals)), nil)

	balance = balance.Mul(balance, big.NewFloat(0).SetInt(pow))

	res, _ := balance.Int(nil)
	return res
}
