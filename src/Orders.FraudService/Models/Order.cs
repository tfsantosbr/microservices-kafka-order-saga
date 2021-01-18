using System;

namespace Orders.FraudService.Models
{
    public class Order
    {
        public Order(int productId, int quantity, decimal price)
        {
            Id = Guid.NewGuid();
            ProductId = productId;
            Quantity = quantity;
            Price = price;
        }

        public Guid Id { get; private set; }
        public int ProductId { get; private set; }
        public int Quantity { get; private set; }
        public decimal Price { get; private set; }
        public bool Valid { get; private set; }

        public void Validate()
        {
            Valid = true;
        }
    }
}